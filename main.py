import asyncio
import logging
import sys
import datetime
import os
import ssl
from contextlib import asynccontextmanager

from aiogram import Bot, Dispatcher, types, Router, F
from aiogram.filters import CommandStart, Command, CommandObject
from aiogram.types import MenuButtonWebApp, WebAppInfo, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import uvicorn
import asyncpg
# ================= НАСТРОЙКИ (ЗАПОЛНИ!) =================
TOKEN_WORKER = "8334327123:AAFdzGoc5LOdN01RLk6p992LcJFsrhOLDpw"
TOKEN_LEAD = "8014174795:AAF4l4PG8xxtPebJs4f3jRaLa2ecWoFaoCk"

ADMIN_ID = 7608729469  # Твой цифровой ID
LEAD_CHAT_ID = -5208153223 # ID чата, куда падает анкета (с минусом!)
ADMIN_USERNAME = "arbixnet" # БЕЗ @ (Куда писать лидам)
# Ссылка на Ngrok (не забудь /app в конце)
WEBAPP_URL = "https://winerarc.onrender.com/app"

# Имя Лид-Бота (чтобы воркер знал, на кого лить)
LEAD_BOT_USERNAME = "arbixnet_bot" # Без @

WELCOME_IMAGE = ""
# ========================================================
DATABASE_URL = "postgresql://postgres.xehqmckhoypehdvcyuqc:1KPi1id9M9VNpDq5@aws-1-eu-west-1.pooler.supabase.com:6543/postgres"
# ========================================================
LEAD_BOT_USERNAME = LEAD_BOT_USERNAME.replace("@", "")

bot_worker = Bot(token=TOKEN_WORKER)
bot_lead = Bot(token=TOKEN_LEAD)
dp_worker = Dispatcher()
dp_lead = Dispatcher()
router_worker = Router()
router_lead = Router()
dp_worker.include_router(router_worker)
dp_lead.include_router(router_lead)

# Глобальная переменная для базы
db_pool = None

async def init_db():
    global db_pool
    print("🔌 Начинаю подключение к базе данных...")

    # Берем ссылку, которую ты вписал выше
    url = DATABASE_URL
    
    # Автоматический фикс для Supabase Transaction Pooler
    # Мы добавляем отключение кэша запросов, иначе будет ошибка
    if "prepared_statement_cache_size" not in url:
        if "?" in url:
            url += "&prepared_statement_cache_size=0"
        else:
            url += "?prepared_statement_cache_size=0"

    try:
        # Настройка SSL (чтобы Supabase пустил)
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        
        # Подключаемся
        db_pool = await asyncpg.create_pool(url, ssl=ctx)
        print("✅ БАЗА ДАННЫХ УСПЕШНО ПОДКЛЮЧЕНА!")
        
        async with db_pool.acquire() as conn:
            await conn.execute('''CREATE TABLE IF NOT EXISTS workers (user_id BIGINT PRIMARY KEY, username TEXT, nickname TEXT, percent INTEGER DEFAULT 60, balance INTEGER DEFAULT 0)''')
            await conn.execute('''CREATE TABLE IF NOT EXISTS links (id SERIAL PRIMARY KEY, worker_id BIGINT, marker TEXT, clicks INTEGER DEFAULT 0, cost INTEGER DEFAULT 0, revenue INTEGER DEFAULT 0, created_at TIMESTAMPTZ DEFAULT NOW(), UNIQUE(worker_id, marker))''')
            await conn.execute('''CREATE TABLE IF NOT EXISTS leads (id SERIAL PRIMARY KEY, user_id BIGINT, username TEXT, first_name TEXT, worker_id BIGINT, marker TEXT, status TEXT DEFAULT 'NEW', info TEXT, push1 INTEGER DEFAULT 0, push2 INTEGER DEFAULT 0, created_at TIMESTAMPTZ DEFAULT NOW())''')
            await conn.execute('''CREATE TABLE IF NOT EXISTS withdrawals (id SERIAL PRIMARY KEY, worker_id BIGINT, amount INTEGER, method TEXT, wallet TEXT, status TEXT DEFAULT 'PENDING', created_at TIMESTAMPTZ DEFAULT NOW())''')
            print("✅ Таблицы готовы.")
            
    except Exception as e:
        print(f"\n❌ ОШИБКА БАЗЫ: {e}")
        print("👉 Проверь: 1) Пароль в ссылке 2) Выбрал ли ты Transaction Pooler (порт 6543)")

# --- ФОНОВАЯ ЗАДАЧА ---
async def leads_followup():
    print("🤖 Followup service started...")
    while True:
        try:
            await asyncio.sleep(60)
            if not db_pool: continue
            
            async with db_pool.acquire() as conn:
                rows = await conn.fetch("SELECT id, user_id, status, created_at, push1, push2 FROM leads WHERE status IN ('NEW', 'READY')")
                now = datetime.datetime.now(datetime.timezone.utc)
                
                for row in rows:
                    lid, uid, status, created_at, p1, p2 = row
                    diff = (now - created_at).total_seconds()

                    if status == 'NEW' and diff > 1800 and not p1:
                        try:
                            kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="➡️ Продолжить", callback_data="start")]])
                            await bot_lead.send_message(uid, "⏳ <b>Вы не закончили верификацию!</b>", parse_mode="HTML", reply_markup=kb)
                            await conn.execute("UPDATE leads SET push1 = 1 WHERE id = $1", lid)
                        except: pass

                    elif status == 'READY' and diff > 86400 and not p2:
                        try:
                            kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔥 Написать куратору", url=f"https://t.me/{ADMIN_USERNAME}")]])
                            await bot_lead.send_message(uid, "👋 <b>Вы с нами?</b>", parse_mode="HTML", reply_markup=kb)
                            await conn.execute("UPDATE leads SET push2 = 1 WHERE id = $1", lid)
                        except: pass

        except Exception as e:
            print(f"Followup Error: {e}")
            await asyncio.sleep(60)

# --- LIFESPAN ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    asyncio.create_task(leads_followup())
    await bot_worker.delete_webhook(drop_pending_updates=True)
    await bot_lead.delete_webhook(drop_pending_updates=True)
    asyncio.create_task(dp_worker.start_polling(bot_worker))
    asyncio.create_task(dp_lead.start_polling(bot_lead))
    yield
    await bot_worker.session.close()
    await bot_lead.session.close()
    if db_pool: await db_pool.close()

app = FastAPI(lifespan=lifespan)
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="static")

@app.get("/")
async def root():
    return RedirectResponse(url="/app")

class LeadForm(StatesGroup):
    age = State(); proxy = State(); timezone = State(); crypto_exp = State()

# --- ВОРКЕР ---
@router_worker.message(CommandStart())
async def worker_start(message: types.Message):
    uid, uname = message.from_user.id, message.from_user.username or "Anon"
    if not db_pool: return await message.answer("❌ Ошибка: База данных не подключена.")
    
    async with db_pool.acquire() as conn:
        await conn.execute("INSERT INTO workers (user_id, username, nickname) VALUES ($1, $2, $3) ON CONFLICT (user_id) DO NOTHING", uid, uname, f"Partner {uid}")
        await conn.execute("UPDATE workers SET username = $1 WHERE user_id = $2", uname, uid)
        await conn.execute("INSERT INTO links (worker_id, marker) VALUES ($1, $2) ON CONFLICT DO NOTHING", uid, "Main")
    
    kb = types.InlineKeyboardMarkup(inline_keyboard=[[types.InlineKeyboardButton(text="💎 Открыть Dashboard", web_app=WebAppInfo(url=WEBAPP_URL))]])
    await message.answer(f"👋 <b>Панель</b>\nID: <code>{uid}</code>", parse_mode="HTML", reply_markup=kb)
    await bot_worker.set_chat_menu_button(chat_id=message.chat.id, menu_button=MenuButtonWebApp(text="📱 CRM", web_app=WebAppInfo(url=WEBAPP_URL)))

@router_worker.message(Command("close"))
async def admin_close(message: types.Message):
    if message.from_user.id != ADMIN_ID: return
    try:
        parts = message.text.split()
        target = parts[1].replace("@", "")
        amount = float(parts[2])
    except: return await message.answer("⚠️ Формат: `/close @user 1000`")
    if not db_pool: return await message.answer("❌ База не подключена")

    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT worker_id, first_name, marker FROM leads WHERE username = $1 ORDER BY id DESC LIMIT 1", target)
        if not row: return await message.answer("❌ Лид не найден")
        wid, fname, marker = row
        
        if wid == 0: 
             await conn.execute("UPDATE leads SET status = 'DEP' WHERE username = $1", target)
             return await message.answer("✅ Органика закрыта.")
        
        res = await conn.fetchrow("SELECT percent FROM workers WHERE user_id = $1", wid)
        percent = res['percent'] if res else 60
        profit = int(amount * (percent / 100))
        
        await conn.execute("UPDATE workers SET balance = balance + $1 WHERE user_id = $2", profit, wid)
        await conn.execute("UPDATE links SET revenue = revenue + $1 WHERE worker_id = $2 AND marker = $3", profit, wid, marker)
        await conn.execute("UPDATE leads SET status = 'DEP' WHERE username = $1", target)

    try: await bot_worker.send_message(wid, f"💸 <b>ДЕПОЗИТ!</b>\nЛид: {fname} ({marker})\nВаш профит: +${profit}", parse_mode="HTML")
    except: pass
    await message.answer(f"✅ Депозит засчитан.")

@router_worker.callback_query(F.data.startswith("pay_"))
async def admin_pay_confirm(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID: return
    w_id_tx = int(call.data.split("_")[1])
    async with db_pool.acquire() as conn:
        tx = await conn.fetchrow("SELECT worker_id, amount FROM withdrawals WHERE id = $1", w_id_tx)
        if not tx: return await call.answer("Заявка не найдена")
        worker_id, amount = tx['worker_id'], tx['amount']
        await conn.execute("UPDATE withdrawals SET status = 'PAID' WHERE id = $1", w_id_tx)
    
    await call.message.edit_text(f"{call.message.text}\n\n✅ <b>ОПЛАЧЕНО</b>", parse_mode="HTML", reply_markup=None)
    try: await bot_worker.send_message(worker_id, f"✅ <b>ВЫПЛАТА ПОДТВЕРЖДЕНА</b>\nСумма: ${amount}", parse_mode="HTML")
    except: pass

@router_worker.message(Command("trash"))
async def admin_trash(message: types.Message):
    if message.from_user.id != ADMIN_ID: return
    try: target = message.text.split()[1].replace("@", "")
    except: return
    async with db_pool.acquire() as conn:
        await conn.execute("UPDATE leads SET status = 'BAD' WHERE username = $1", target)
    await message.answer(f"🗑 Лид @{target} — БРАК.")

# --- ЛИД БОТ ---
@router_lead.message(CommandStart())
async def lead_start(message: types.Message, command: CommandObject, state: FSMContext):
    await state.clear()
    if not db_pool: return
    
    ref = command.args
    w_id = 0
    mark = "Organic"
    
    if ref:
        try:
            if "_" in ref:
                parts = ref.split("_", 1)
                if parts[0].isdigit(): w_id, mark = int(parts[0]), parts[1]
            elif ref.isdigit(): w_id, mark = int(ref), "Main"
            
            if w_id > 0:
                async with db_pool.acquire() as conn:
                    await conn.execute("INSERT INTO links (worker_id, marker) VALUES ($1, $2) ON CONFLICT DO NOTHING", w_id, mark)
                    await conn.execute("UPDATE links SET clicks = clicks + 1 WHERE worker_id = $1 AND marker = $2", w_id, mark)
        except: pass

    uid = message.from_user.id
    uname = message.from_user.username or "Hidden"
    fname = message.from_user.first_name
    
    async with db_pool.acquire() as conn:
        exists = await conn.fetchval("SELECT id FROM leads WHERE user_id = $1", uid)
        if not exists:
            await conn.execute("INSERT INTO leads (user_id, username, first_name, worker_id, marker, status, info) VALUES ($1, $2, $3, $4, $5, 'NEW', 'Started')", uid, uname, fname, w_id, mark)

    await state.update_data(wid=w_id, mrk=mark)
    txt = "<b>Arbitrage Team | Verification</b>\n\nПриветствуем. Пройдите верификацию."
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Начать верификацию", callback_data="start")]])
    try: await message.answer_photo(WELCOME_IMAGE, caption=txt, parse_mode="HTML", reply_markup=kb)
    except: await message.answer(txt, parse_mode="HTML", reply_markup=kb)

async def safe_edit(call, text, kb=None):
    try: await call.message.edit_caption(caption=text, reply_markup=kb, parse_mode="HTML")
    except: await call.message.edit_text(text=text, reply_markup=kb, parse_mode="HTML")

@router_lead.callback_query(lambda c: c.data == "start")
async def start_v(call: types.CallbackQuery, state: FSMContext):
    await safe_edit(call, "🔞 <b>Возраст?</b>", InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="< 18", callback_data="no"), InlineKeyboardButton(text="18+", callback_data="ok")]]))
    await state.set_state(LeadForm.age)

@router_lead.callback_query(LeadForm.age)
async def q_age(call: types.CallbackQuery, state: FSMContext):
    if call.data == "ok":
        await safe_edit(call, "🌍 <b>Часовой пояс?</b>", None); await state.set_state(LeadForm.timezone)
    else:
        await safe_edit(call, "⚠️ <b>Compliance</b>\nЕсть представитель 18+?", InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Да", callback_data="ok"), InlineKeyboardButton(text="Нет", callback_data="fail")]]))
        await state.set_state(LeadForm.proxy)

@router_lead.callback_query(LeadForm.proxy)
async def q_proxy(call: types.CallbackQuery, state: FSMContext):
    if call.data == "ok": await safe_edit(call, "🌍 <b>Часовой пояс?</b>", None); await state.set_state(LeadForm.timezone)
    else: await safe_edit(call, "⛔️ Доступ закрыт.", None); await state.clear()

@router_lead.message(LeadForm.timezone)
async def q_zone(message: types.Message, state: FSMContext):
    await state.update_data(zone=message.text)
    await message.answer("💎 <b>Опыт в крипте?</b>", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Да", callback_data="yes"), InlineKeyboardButton(text="Нет", callback_data="no")]]), parse_mode="HTML")
    await state.set_state(LeadForm.crypto_exp)

@router_lead.callback_query(LeadForm.crypto_exp)
async def q_fin(call: types.CallbackQuery, state: FSMContext):
    d = await state.get_data()
    uid, uname, fname = call.from_user.id, call.from_user.username or "Hidden", call.from_user.first_name
    w_id, marker, info = d.get('wid'), d.get('mrk'), f"Zone: {d.get('zone')}, Exp: {call.data}"
    
    async with db_pool.acquire() as conn:
        await conn.execute("UPDATE leads SET status = 'READY', info = $1, first_name = $2, username = $3 WHERE user_id = $4", info, fname, uname, uid)
        
        worker_info = f"ID: <code>{w_id}</code>"
        if w_id > 0:
            row = await conn.fetchrow("SELECT nickname, username FROM workers WHERE user_id = $1", w_id)
            if row: worker_info = f"<b>{row['nickname']}</b> (@{row['username']}) | ID: <code>{w_id}</code>"

    msg_text = (f"🚀 <b>НОВЫЙ ЛИД!</b>\n👤 <b>Юзер:</b> @{uname}\n👨‍💻 <b>Воркер:</b> {worker_info}\n🔗 <b>Метка:</b> {marker}\nℹ️ <b>Инфо:</b> {info}")
    try: await bot_lead.send_message(LEAD_CHAT_ID, msg_text, parse_mode="HTML")
    except: pass
    await safe_edit(call, f"✅ <b>Заявка одобрена.</b>\n\nКуратор: @{ADMIN_USERNAME}\nВаш код: <code>{uid}</code>", InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="👨‍💻 Связаться с Куратором", url=f"https://t.me/{ADMIN_USERNAME}")]]))
    await state.clear()

# --- API ---
@app.get("/app", response_class=HTMLResponse)
async def get_app(request: Request): return templates.TemplateResponse("index.html", {"request": request})

@app.get("/api/user/{user_id}")
async def get_user(user_id: int, period: str = "all"):
    if not db_pool: return {"error": "DB not connected"}
    async with db_pool.acquire() as conn:
        w = await conn.fetchrow("SELECT nickname, balance, percent FROM workers WHERE user_id=$1", user_id)
        if not w: return {"error": "User not found"}
        
        time_filter = ""
        if period == "day": time_filter = "AND created_at >= NOW() - INTERVAL '1 day'"
        elif period == "week": time_filter = "AND created_at >= NOW() - INTERVAL '7 days'"
        
        paid_res = await conn.fetchval("SELECT SUM(amount) FROM withdrawals WHERE worker_id=$1 AND status='PAID'", user_id)
        total_paid = paid_res if paid_res else 0
        total_earned = w['balance'] + total_paid

        sql_links = f'''SELECT marker, clicks, cost, revenue, 
            (SELECT COUNT(*) FROM leads WHERE worker_id=l.worker_id AND marker=l.marker {time_filter}) as leads, 
            (SELECT COUNT(*) FROM leads WHERE worker_id=l.worker_id AND marker=l.marker AND status='DEP' {time_filter}) as deps
            FROM links l WHERE worker_id=$1 ORDER BY id DESC'''
        links = [{"marker": r['marker'], "clicks": r['clicks'], "cost": r['cost'], "revenue": r['revenue'], "leads": r['leads'], "deps": r['deps']} for r in await conn.fetch(sql_links, user_id)]
        
        sql_leads = f"SELECT username, status, marker, created_at FROM leads WHERE worker_id=$1 AND status!='BAD' {time_filter} ORDER BY id DESC LIMIT 50"
        leads = []
        for r in await conn.fetch(sql_leads, user_id):
            leads.append({"username": r['username'], "status": r['status'], "marker": r['marker'], "date": r['created_at'].strftime('%Y-%m-%d %H:%M')})
        
        # График
        chart_data = {"labels": [], "leads": [], "deps": []}
        today = datetime.date.today()
        for i in range(6, -1, -1):
            d = today - datetime.timedelta(days=i)
            c_leads = await conn.fetchval("SELECT COUNT(*) FROM leads WHERE worker_id=$1 AND created_at::date=$2", user_id, d)
            c_deps = await conn.fetchval("SELECT COUNT(*) FROM leads WHERE worker_id=$1 AND status='DEP' AND created_at::date=$2", user_id, d)
            chart_data["labels"].append(d.strftime('%m-%d'))
            chart_data["leads"].append(c_leads)
            chart_data["deps"].append(c_deps)

        sql_w = "SELECT amount, method, wallet, status, created_at FROM withdrawals WHERE worker_id=$1 ORDER BY id DESC"
        withdrawals = [{"amount": r['amount'], "method": r['method'], "wallet": r['wallet'], "status": r['status'], "date": r['created_at'].strftime('%Y-%m-%d %H:%M')} for r in await conn.fetch(sql_w, user_id)]

        return {
            "nickname": w['nickname'], "balance": w['balance'], "total_earned": total_earned, "percent": w['percent'], "status": "Worker",
            "bot_username": LEAD_BOT_USERNAME, "leads": leads, "links": links,
            "chart": chart_data, "withdrawals": withdrawals, "support_link": f"https://t.me/{ADMIN_USERNAME}"
        }

@app.post("/api/action")
async def api_act(request: Request):
    if not db_pool: return {"status": "error_db"}
    d = await request.json()
    act, uid = d.get("action"), d.get("user_id")
    async with db_pool.acquire() as conn:
        if act == "create_link":
            try: await conn.execute("INSERT INTO links (worker_id, marker) VALUES ($1, $2)", uid, d.get("marker"))
            except: return {"status": "error"}
        elif act == "update_cost":
            await conn.execute("UPDATE links SET cost = $1 WHERE worker_id = $2 AND marker = $3", d.get("cost"), uid, d.get("marker"))
        elif act == "set_nick":
            await conn.execute("UPDATE workers SET nickname = $1 WHERE user_id = $2", d.get("nickname"), uid)
        elif act == "withdraw":
            amount = int(d.get("amount"))
            method, wallet = d.get("method"), d.get("wallet")
            row = await conn.fetchrow("SELECT balance, username, nickname FROM workers WHERE user_id = $1", uid)
            if not row or row['balance'] < amount: return {"status": "no_balance"}
            
            await conn.execute("UPDATE workers SET balance = balance - $1 WHERE user_id = $2", amount, uid)
            val = await conn.fetchval("INSERT INTO withdrawals (worker_id, amount, method, wallet) VALUES ($1, $2, $3, $4) RETURNING id", uid, amount, method, wallet)
            tx_id = val
            
            kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✅ Подтвердить выплату", callback_data=f"pay_{tx_id}")]])
            msg = f"💸 <b>ЗАЯВКА НА ВЫВОД</b>\nВоркер: {row['nickname']} (@{row['username']})\nСумма: <b>${amount}</b>\nМетод: {method}\nКошелек: <code>{wallet}</code>"
            try: await bot_worker.send_message(ADMIN_ID, msg, parse_mode="HTML", reply_markup=kb)
            except: pass
    return {"status": "ok"}

@app.get("/api/top")
async def get_top():
    if not db_pool: return []
    async with db_pool.acquire() as conn:
        sql = '''SELECT w.nickname, (w.balance + COALESCE((SELECT SUM(amount) FROM withdrawals WHERE worker_id=w.user_id AND status='PAID'), 0)) as total 
                 FROM workers w ORDER BY total DESC LIMIT 10'''
        res = await conn.fetch(sql)
        return [{"nickname": r['nickname'], "balance": r['total']} for r in res]

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
