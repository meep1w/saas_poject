# parent_bot.py
from __future__ import annotations

import asyncio
from typing import List, Tuple

from aiogram import Bot, Dispatcher, Router, F
from aiogram.client.default import DefaultBotProperties
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import Command
from aiogram.exceptions import TelegramForbiddenError

from sqlalchemy import select, func

from app.settings import settings
from app.db import SessionLocal
from app.models import Tenant, UserAccess, Event

router = Router()

WELCOME_OK_RU = (
    "Привет! Вы член моей приватки — доступ разрешён.\n"
    "Отправьте API-ТОКЕН вашего бота, которого хотите подключить.\n"
    "Важно: можно подключить только *1 бота*."
)
WELCOME_NO_RU = (
    "Извините, вы не находитесь в моей приватке. Напишите мне для уточнения информации."
)

# =========================
#     Обычный parent /start
# =========================
@router.message(Command("start"))
async def on_start(m: Message):
    user_id = m.from_user.id
    try:
        member = await m.bot.get_chat_member(settings.PRIVATE_CHANNEL_ID, user_id)
        status = getattr(member, "status", None)
        if status in {"creator", "administrator", "member"}:
            await m.answer(WELCOME_OK_RU)
        else:
            await m.answer(WELCOME_NO_RU)
            return
    except TelegramForbiddenError:
        await m.answer(
            "Я не админ в приватном канале. Добавьте меня админом и повторите."
        )
        return

    async with SessionLocal() as s:
        res = await s.execute(select(Tenant).where(Tenant.owner_telegram_id == user_id))
        tenant = res.scalar_one_or_none()
        if tenant and tenant.bot_username:
            await m.answer(
                f"У вас уже подключён бот @{tenant.bot_username}. "
                f"Если хотите заменить — отправьте новый токен, старый будет отключён."
            )

@router.message(F.text.regexp(r"^\d{6,}:[A-Za-z0-9_-]{20,}$"))
async def on_token(m: Message):
    token = m.text.strip()
    user_id = m.from_user.id

    test_bot = Bot(token, default=DefaultBotProperties(parse_mode="HTML"))
    try:
        me = await test_bot.get_me()
    except Exception:
        await m.answer("Токен невалиден. Проверьте и пришлите снова.")
        await test_bot.session.close()
        return

    username = me.username or ""
    await test_bot.session.close()

    async with SessionLocal() as s:
        res = await s.execute(select(Tenant).where(Tenant.owner_telegram_id == user_id))
        tenant = res.scalar_one_or_none()
        if tenant is None:
            tenant = Tenant(
                owner_telegram_id=user_id,
                bot_token=token,
                bot_username=username,
                is_active=True,
            )
            s.add(tenant)
        else:
            tenant.bot_token = token
            tenant.bot_username = username
            tenant.is_active = True
        await s.commit()

    await m.answer(
        "Ваш бот подключен! Перейдите в него для проверки работы: "
        f"https://t.me/{username if username else 'your_bot'}"
    )

# =========================================================
#                Глобальная админка: /ga
# =========================================================

PAGE_SIZE = 10

def _ga_admin_ids() -> List[int]:
    """
    Список супер-админов берём из settings.GA_ADMIN_IDS.
    Может быть либо списком int, либо строкой "123,456".
    """
    v = getattr(settings, "GA_ADMIN_IDS", [])
    if isinstance(v, str):
        try:
            return [int(x) for x in v.replace(" ", "").split(",") if x]
        except Exception:
            return []
    return list(v or [])

def _is_ga(user_id: int) -> bool:
    return user_id in _ga_admin_ids()

def _kb_ga_main(tenants_count: int) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text=f"🧩 Тенанты ({tenants_count})", callback_data="ga:tenants:0")],
        [InlineKeyboardButton(text="🔄 Deploy", callback_data="ga:deploy"),
         InlineKeyboardButton(text="♻️ Restart", callback_data="ga:restart")],
        [InlineKeyboardButton(text="🔁 Обновить", callback_data="ga:refresh")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)

def _kb_tenants(items: List[Tenant], page: int, more: bool) -> InlineKeyboardMarkup:
    rows = []
    for t in items:
        cap = f"{t.id} • @{t.bot_username or '—'}"
        rows.append([InlineKeyboardButton(text=cap, callback_data=f"ga:tenant:{t.id}")])
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="⬅️ Назад", callback_data=f"ga:tenants:{page-1}"))
    if more:
        nav.append(InlineKeyboardButton(text="Вперёд ➡️", callback_data=f"ga:tenants:{page+1}"))
    if nav:
        rows.append(nav)
    rows.append([InlineKeyboardButton(text="↩️ Главное меню", callback_data="ga:menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def _kb_tenant_card(t: Tenant) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text="🔄 Deploy", callback_data=f"ga:tenant:{t.id}:deploy"),
         InlineKeyboardButton(text="♻️ Restart service", callback_data=f"ga:tenant:{t.id}:restart")],
        [InlineKeyboardButton(text="⏹ Stop service", callback_data=f"ga:tenant:{t.id}:stop")],
        [InlineKeyboardButton(text="🧹 Очистить БД", callback_data=f"ga:tenant:{t.id}:clear")],
        [InlineKeyboardButton(text="🗑 Удалить тенант", callback_data=f"ga:tenant:{t.id}:delete")],
        [InlineKeyboardButton(text="↩️ К списку", callback_data="ga:tenants:0"),
         InlineKeyboardButton(text="↩️ Меню", callback_data="ga:menu")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)

def _kb_confirm(cb_yes: str, back_cb: str, caption_yes: str = "✅ Да, подтвердить") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=caption_yes, callback_data=cb_yes)],
        [InlineKeyboardButton(text="↩️ Отмена", callback_data=back_cb)],
    ])

async def _run_shell(cmd: str) -> Tuple[bool, str]:
    """
    Запускаем shell-команду; нужен доступ (sudoers без пароля для указанных команд).
    Возвращаем (ok, tail_log).
    """
    proc = await asyncio.create_subprocess_shell(
        cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    out, _ = await proc.communicate()
    text = (out or b"").decode("utf-8", errors="ignore")
    return (proc.returncode == 0), (text[-4000:] if text else "")

async def _stats_text() -> Tuple[str, int]:
    async with SessionLocal() as s:
        tenants_count = (await s.execute(select(func.count()).select_from(Tenant))).scalar() or 0
        users_rows = (await s.execute(select(func.count()).select_from(UserAccess))).scalar() or 0
        uniq_users = (await s.execute(select(func.count(func.distinct(UserAccess.user_id))).select_from(UserAccess))).scalar() or 0
        dep_sum = (await s.execute(
            select(func.coalesce(func.sum(Event.amount), 0.0)).where(Event.kind.in_(("ftd", "rd")))
        )).scalar() or 0.0
        dep_cnt = (await s.execute(
            select(func.count()).select_from(Event).where(Event.kind.in_(("ftd", "rd")))
        )).scalar() or 0

    txt = (
        "🧠 Global Admin\n\n"
        f"• Тенантов: {tenants_count}\n"
        f"• Пользователей (строк): {users_rows}\n"
        f"• Уникальных TG ID: {uniq_users}\n"
        f"• Депозитов: {dep_cnt}\n"
        f"• Сумма депозитов: {dep_sum:.2f}$\n"
    )
    return txt, tenants_count

async def _tenants_page(page: int) -> Tuple[List[Tenant], bool, int]:
    async with SessionLocal() as s:
        total = (await s.execute(select(func.count()).select_from(Tenant))).scalar() or 0
        res = await s.execute(
            select(Tenant).order_by(Tenant.id.desc()).offset(page * PAGE_SIZE).limit(PAGE_SIZE)
        )
        items = res.scalars().all()
    more = (page + 1) * PAGE_SIZE < total
    return items, more, total

# -------- /ga entry --------
@router.message(Command("ga"))
async def ga_entry(m: Message):
    if not _is_ga(m.from_user.id):
        await m.answer("Доступ запрещён.")
        return
    txt, tenants_count = await _stats_text()
    await m.answer(txt, reply_markup=_kb_ga_main(tenants_count))

@router.callback_query(F.data == "ga:menu")
async def ga_menu(c: CallbackQuery):
    if not _is_ga(c.from_user.id): return
    txt, tenants_count = await _stats_text()
    await c.message.edit_text(txt, reply_markup=_kb_ga_main(tenants_count))
    await c.answer()

@router.callback_query(F.data == "ga:refresh")
async def ga_refresh(c: CallbackQuery):
    if not _is_ga(c.from_user.id): return
    await ga_menu(c)

# -------- tenants list --------
@router.callback_query(F.data.startswith("ga:tenants:"))
async def ga_tenants(c: CallbackQuery):
    if not _is_ga(c.from_user.id): return
    page = int(c.data.split(":")[2])
    items, more, total = await _tenants_page(page)
    cap = f"🧩 Тенанты ({total})\nВыберите бота:"
    await c.message.edit_text(cap, reply_markup=_kb_tenants(items, page, more))
    await c.answer()

# -------- tenant card (view) --------
@router.callback_query(F.data.startswith("ga:tenant:") & ~F.data.endswith((":deploy", ":restart", ":stop", ":clear", ":clear:yes", ":delete", ":delete:yes")))
async def ga_tenant_card(c: CallbackQuery):
    if not _is_ga(c.from_user.id): return
    tid = int(c.data.split(":")[2])
    async with SessionLocal() as s:
        t = (await s.execute(select(Tenant).where(Tenant.id == tid))).scalar_one_or_none()
    if not t:
        await c.answer("Тенант не найден", show_alert=True)
        return
    lines = [
        f"🧾 Тенант #{t.id}",
        f"@{t.bot_username or '—'}",
        "",
        f"Owner TG: {getattr(t, 'owner_telegram_id', None) or '—'}",
        f"Support URL: {t.support_url or '—'}",
        f"Channel ID: {t.gate_channel_id or '—'}",
        f"Channel URL: {t.gate_channel_url or '—'}",
        f"Ref link: {t.ref_link or '—'}",
        f"Deposit link: {t.deposit_link or '—'}",
        f"Check subscription: {bool(getattr(t, 'check_subscription', True))}",
        f"Check deposit: {bool(getattr(t, 'check_deposit', True))}",
        f"Min deposit $: {int(getattr(t, 'min_deposit_usd', 0) or 0)}",
        f"Platinum threshold $: {int(getattr(t, 'platinum_threshold_usd', 500) or 500)}",
        f"PB secret set: {bool(t.pb_secret)}",
    ]
    txt = "\n".join(lines)
    await c.message.edit_text(txt, reply_markup=_kb_tenant_card(t))
    await c.answer()

# -------- global actions --------
@router.callback_query(F.data == "ga:deploy")
async def ga_deploy(c: CallbackQuery):
    if not _is_ga(c.from_user.id): return
    await c.answer("Запускаю деплой…")
    ok, log = await _run_shell("sudo /usr/local/bin/pocket_deploy")
    await c.message.answer("✅ Деплой завершён" if ok else "❌ Деплой не прошёл")
    if log:
        await c.message.answer(f"<code>{log}</code>")

@router.callback_query(F.data == "ga:restart")
async def ga_restart(c: CallbackQuery):
    if not _is_ga(c.from_user.id): return
    await c.answer("Рестартую сервисы…")
    ok, log = await _run_shell("sudo systemctl restart pocket-children && sudo systemctl restart pocket-api")
    await c.message.answer("♻️ Рестарт выполнен" if ok else "❌ Рестарт не удался")
    if log:
        await c.message.answer(f"<code>{log}</code>")

# -------- per-tenant actions --------
@router.callback_query(F.data.endswith(":deploy") & F.data.startswith("ga:tenant:"))
async def ga_t_deploy(c: CallbackQuery):
    if not _is_ga(c.from_user.id): return
    tid = int(c.data.split(":")[2])
    await c.answer(f"Деплой (тенант #{tid})…")
    # Пока деплой общий
    ok, log = await _run_shell("sudo /usr/local/bin/pocket_deploy")
    await c.message.answer(f"✅ Деплой (тенант #{tid}) завершён" if ok else f"❌ Деплой (тенант #{tid}) упал")
    if log:
        await c.message.answer(f"<code>{log}</code>")

@router.callback_query(F.data.endswith(":restart") & F.data.startswith("ga:tenant:"))
async def ga_t_restart(c: CallbackQuery):
    if not _is_ga(c.from_user.id): return
    tid = int(c.data.split(":")[2])
    await c.answer(f"Рестарт сервисов для тенанта #{tid}…")
    ok, log = await _run_shell("sudo systemctl restart pocket-children")
    await c.message.answer(f"♻️ Рестарт (тенант #{tid}) выполнен" if ok else f"❌ Рестарт (тенант #{tid}) не удался")
    if log:
        await c.message.answer(f"<code>{log}</code>")

@router.callback_query(F.data.endswith(":stop") & F.data.startswith("ga:tenant:"))
async def ga_t_stop(c: CallbackQuery):
    if not _is_ga(c.from_user.id): return
    tid = int(c.data.split(":")[2])
    await c.message.answer(
        f"⚠️ Остановить сервис для тенанта #{tid}? Это остановит всех ботов.",
        reply_markup=_kb_confirm(
            cb_yes=f"ga:tenant:{tid}:stop:yes",
            back_cb=f"ga:tenant:{tid}",
            caption_yes="⏹ Да, остановить pocket-children",
        )
    )
    await c.answer()

@router.callback_query(F.data.endswith(":stop:yes") & F.data.startswith("ga:tenant:"))
async def ga_t_stop_yes(c: CallbackQuery):
    if not _is_ga(c.from_user.id): return
    ok, log = await _run_shell("sudo systemctl stop pocket-children")
    await c.message.answer("⏹ Остановлено" if ok else "❌ Команда не удалась")
    if log:
        await c.message.answer(f"<code>{log}</code>")
    await c.answer()

@router.callback_query(F.data.endswith(":clear") & F.data.startswith("ga:tenant:"))
async def ga_t_clear(c: CallbackQuery):
    if not _is_ga(c.from_user.id): return
    tid = int(c.data.split(":")[2])
    await c.message.answer(
        f"🧹 Очистить БД для тенанта #{tid}? Удалятся users, events, overrides.",
        reply_markup=_kb_confirm(cb_yes=f"ga:tenant:{tid}:clear:yes", back_cb=f"ga:tenant:{tid}")
    )
    await c.answer()

@router.callback_query(F.data.endswith(":clear:yes") & F.data.startswith("ga:tenant:"))
async def ga_t_clear_yes(c: CallbackQuery):
    if not _is_ga(c.from_user.id): return
    tid = int(c.data.split(":")[2])
    async with SessionLocal() as s:
        await s.execute(UserAccess.__table__.delete().where(UserAccess.tenant_id == tid))
        await s.execute(Event.__table__.delete().where(Event.tenant_id == tid))
        # опционально — если модели присутствуют в вашем проекте:
        try:
            from app.models import ContentOverride, UserLang, UserState
            await s.execute(ContentOverride.__table__.delete().where(ContentOverride.tenant_id == tid))
            await s.execute(UserLang.__table__.delete().where(UserLang.tenant_id == tid))
            await s.execute(UserState.__table__.delete().where(UserState.tenant_id == tid))
        except Exception:
            pass
        await s.commit()
    await c.message.answer(f"🧹 Готово: данные тенанта #{tid} очищены.")
    await c.answer()

@router.callback_query(F.data.endswith(":delete") & F.data.startswith("ga:tenant:"))
async def ga_t_delete(c: CallbackQuery):
    if not _is_ga(c.from_user.id): return
    tid = int(c.data.split(":")[2])
    await c.message.answer(
        f"🗑 Удалить тенант #{tid} полностью? Это необратимо.",
        reply_markup=_kb_confirm(cb_yes=f"ga:tenant:{tid}:delete:yes", back_cb=f"ga:tenant:{tid}")
    )
    await c.answer()

@router.callback_query(F.data.endswith(":delete:yes") & F.data.startswith("ga:tenant:"))
async def ga_t_delete_yes(c: CallbackQuery):
    if not _is_ga(c.from_user.id): return
    tid = int(c.data.split(":")[2])
    async with SessionLocal() as s:
        await s.execute(UserAccess.__table__.delete().where(UserAccess.tenant_id == tid))
        await s.execute(Event.__table__.delete().where(Event.tenant_id == tid))
        try:
            from app.models import ContentOverride, UserLang, UserState
            await s.execute(ContentOverride.__table__.delete().where(ContentOverride.tenant_id == tid))
            await s.execute(UserLang.__table__.delete().where(UserLang.tenant_id == tid))
            await s.execute(UserState.__table__.delete().where(UserState.tenant_id == tid))
        except Exception:
            pass
        await s.execute(Tenant.__table__.delete().where(Tenant.id == tid))
        await s.commit()
    await c.message.answer(f"🗑 Тенант #{tid} удалён.")
    await c.answer()

# =========================
#          Runner
# =========================
async def run_parent():
    bot = Bot(settings.PARENT_BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
    dp = Dispatcher()
    dp.include_router(router)
    await dp.start_polling(bot)
