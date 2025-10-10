# app/bots/parent_bot.py
from __future__ import annotations

import asyncio
import html
from datetime import datetime

from aiogram import Bot, Dispatcher, Router, F
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.exceptions import TelegramForbiddenError

from sqlalchemy import select, func

from app.settings import settings
from app.db import SessionLocal
from app.models import (
    Tenant, UserAccess, Event,
    ContentOverride, UserLang, UserState,
)

router = Router()

# ----------------- GA / деплой / сервисы -----------------
REPO_DIR = "/opt/pocket_saas"                 # рабочая папка проекта (если деплой-скрипту нужна)
DEPLOY_CMD = "/usr/local/bin/pocket_deploy"   # твой деплой-скрипт
CHILD_SERVICE = "pocket-children"             # systemd unit с детскими ботами

PAGE_SIZE = 8


async def _run(cmd: str, cwd: str | None = None) -> tuple[int, str]:
    """Запуск shell-команды и возврат (код_выхода, stdout+stderr)."""
    proc = await asyncio.create_subprocess_shell(
        cmd, cwd=cwd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    out, _ = await proc.communicate()
    text = out.decode(errors="ignore") if out else ""
    return proc.returncode, text


def _is_ga(uid: int) -> bool:
    return uid in set(settings.GA_ADMIN_IDS or [])


def _fmt_money(x: float | int | None) -> str:
    try:
        val = float(x or 0)
        return f"${int(val)}" if abs(val - int(val)) < 1e-9 else f"${val:.2f}"
    except Exception:
        return "$0"


# ----------------- обычное /start + привязка токена -----------------
WELCOME_OK_RU = (
    "Привет! Вижу ты из приватки — доступ разрешён.\n\n"
    "Отправь API-токен твоего бота, которого хочешь подключить.\n"
    "Важно: можно подключить только <b>1 бота</b>."
)
WELCOME_NO_RU = (
    "Извините, вы не находитесь в моей приватке. Напишите мне для уточнения информации."
)


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
        await m.answer("Я не админ в приватном канале. Добавьте меня админом и повторите.")
        return
    except Exception:
        # на случай, если PRIVATE_CHANNEL_ID не задан/некорректен
        await m.answer(WELCOME_NO_RU)
        return

    async with SessionLocal() as s:
        res = await s.execute(select(Tenant).where(Tenant.owner_telegram_id == user_id))
        tenant = res.scalar_one_or_none()
        if tenant and tenant.bot_username:
            await m.answer(
                f"У вас уже подключён бот @{tenant.bot_username}. "
                f"Если хотите заменить — напишите мне, сделаем замену: старый будет отключён."
            )


@router.message(F.text.regexp(r"^\d{6,}:[A-Za-z0-9_-]{20,}$"))
async def on_token(m: Message):
    token = (m.text or "").strip()
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

    link = f"https://t.me/{username}" if username else "https://t.me/"
    await m.answer(
        "Ваш бот подключён! Перейдите и продолжите настройку уже там: "
        f"{link}"
    )


# ----------------- Сверх-админка /ga -----------------
def _kb_ga_home(tenants, page: int, more: bool) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    # верхний ряд: Деплой / Рестарт
    rows.append([
        InlineKeyboardButton(text="🚀 Деплой", callback_data="ga:deploy"),
        InlineKeyboardButton(text="🔄 Рестарт детей", callback_data="ga:restart_children"),
    ])
    # список тенантов
    for t in tenants:
        name = f"@{t.bot_username}" if t.bot_username else f"id={t.id}"
        badge = "🟢" if t.is_active else "⏸"
        rows.append([InlineKeyboardButton(text=f"{badge} Тенант #{t.id} {name}", callback_data=f"ga:t:{t.id}")])
    # навигация
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="⬅️ Назад", callback_data=f"ga:home:{page-1}"))
    if more:
        nav.append(InlineKeyboardButton(text="Вперёд ➡️", callback_data=f"ga:home:{page+1}"))
    if nav:
        rows.append(nav)
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _kb_tenant_card(t: Tenant) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    if t.is_active:
        rows.append([InlineKeyboardButton(text="⏸ Пауза", callback_data=f"ga:t:pause:{t.id}")])
    else:
        rows.append([InlineKeyboardButton(text="▶️ Старт", callback_data=f"ga:t:start:{t.id}")])
    rows.append([InlineKeyboardButton(text="🔁 Рестарт детей", callback_data=f"ga:t:restart:{t.id}")])
    # вспомогательные
    rows.append([InlineKeyboardButton(text="🧹 Удалить", callback_data=f"ga:t:delete_confirm:{t.id}")])
    rows.append([InlineKeyboardButton(text="🏠 Меню", callback_data="ga:home:0")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


@router.message(Command("ga"))
async def ga_entry(m: Message):
    if not _is_ga(m.from_user.id):
        return
    await _ga_home(m)


async def _ga_home(m_or_c: Message | CallbackQuery, page: int = 0):
    # суммарная статистика
    async with SessionLocal() as s:
        t_count = (await s.execute(select(func.count()).select_from(Tenant))).scalar() or 0
        u_total = (await s.execute(select(func.count()).select_from(UserAccess))).scalar() or 0
        u_reg = (await s.execute(
            select(func.count()).select_from(UserAccess).where(UserAccess.is_registered == True)
        )).scalar() or 0
        u_dep = (await s.execute(
            select(func.count()).select_from(UserAccess).where(UserAccess.has_deposit == True)
        )).scalar() or 0
        u_vip = (await s.execute(
            select(func.count()).select_from(UserAccess).where(UserAccess.is_platinum == True)
        )).scalar() or 0
        dep_sum = (await s.execute(
            select(func.coalesce(func.sum(Event.amount), 0.0)).where(Event.kind.in_(("ftd", "rd")))
        )).scalar() or 0.0

        res = await s.execute(
            select(Tenant).order_by(Tenant.id.asc()).offset(page * PAGE_SIZE).limit(PAGE_SIZE)
        )
        tenants = res.scalars().all()
        more = (page + 1) * PAGE_SIZE < t_count

    txt = (
        "📊 <b>Глобальная статистика</b>\n"
        f"Тенантов: {t_count}\n"
        f"Пользователей: {u_total}\n"
        f"Регистраций: {u_reg}\n"
        f"С депозитом: {u_dep}\n"
        f"Platinum: {u_vip}\n"
        f"Сумма депозитов: {_fmt_money(dep_sum)}\n\n"
        "Выберите тенанта:"
    )
    kb = _kb_ga_home(tenants, page, more)

    if isinstance(m_or_c, CallbackQuery):
        await m_or_c.message.edit_text(txt, reply_markup=kb)
        await m_or_c.answer()
    else:
        await m_or_c.answer(txt, reply_markup=kb)


@router.callback_query(F.data.startswith("ga:home:"))
async def ga_home_cb(c: CallbackQuery):
    if not _is_ga(c.from_user.id):
        return
    page = int(c.data.split(":")[2])
    await _ga_home(c, page=page)


# ---- Deploy / Restart (глобальные) ----
@router.callback_query(F.data == "ga:deploy")
async def ga_deploy(c: CallbackQuery):
    if not _is_ga(c.from_user.id):
        return
    await c.answer("Запускаю деплой…")
    code, out = await _run(DEPLOY_CMD, cwd=REPO_DIR)
    tail = html.escape("\n".join(out.strip().splitlines()[-25:]))

    # -15 == SIGTERM → скрипт убит рестартом сервисов — это ожидаемо
    if code in (0, -15):
        await c.message.answer(f"✅ Деплой завершён.\n<pre>{tail}</pre>")
    else:
        await c.message.answer(f"❌ Деплой завершился с ошибкой (exit {code}).\n<pre>{tail}</pre>")


@router.callback_query(F.data == "ga:restart_children")
async def ga_restart_children(c: CallbackQuery):
    if not _is_ga(c.from_user.id):
        return
    await c.answer("Перезапускаю детей…")
    code, out = await _run(f"systemctl restart {CHILD_SERVICE}")
    if code == 0:
        await c.message.answer("✅ Дети перезапущены.")
    else:
        tail = html.escape("\n".join(out.strip().splitlines()[-20:]))
        await c.message.answer(f"❌ Не удалось перезапустить: exit {code}\n<pre>{tail}</pre>")


# ---- Карточка тенанта ----
async def _tenant_stats(tid: int) -> dict:
    async with SessionLocal() as s:
        total = (await s.execute(select(func.count()).select_from(UserAccess).where(UserAccess.tenant_id == tid))).scalar() or 0
        regs = (await s.execute(select(func.count()).select_from(UserAccess).where(UserAccess.tenant_id == tid, UserAccess.is_registered == True))).scalar() or 0
        deps = (await s.execute(select(func.count()).select_from(UserAccess).where(UserAccess.tenant_id == tid, UserAccess.has_deposit == True))).scalar() or 0
        plats = (await s.execute(select(func.count()).select_from(UserAccess).where(UserAccess.tenant_id == tid, UserAccess.is_platinum == True))).scalar() or 0
        dep_sum = (await s.execute(
            select(func.coalesce(func.sum(Event.amount), 0.0)).where(Event.tenant_id == tid, Event.kind.in_(("ftd", "rd")))
        )).scalar() or 0.0
    return {"total": total, "regs": regs, "deps": deps, "plats": plats, "sum": dep_sum}


def _format_tenant_card(t: Tenant, st: dict) -> str:
    lines = [
        f"📦 <b>Тенант #{t.id}</b>",
        f"Имя: {t.bot_username or '—'}",
        f"Статус: {'ACTIVE' if t.is_active else 'PAUSED'}",
        f"Admin ID: {t.owner_telegram_id}",
        f"Канал (обяз.): {t.gate_channel_id or 'None'} / {t.gate_channel_url or 'None'}",
        f"Поддержка: {t.support_url or 'None'}",
        f"Порог Platinum: {int(t.platinum_threshold_usd or 0)}",
        "",
        "📈 <b>Статистика</b>",
        f"Пользователей: {st['total']}",
        f"Регистраций: {st['regs']}",
        f"С депозитом: {st['deps']}",
        f"Platinum: {st['plats']}",
        f"Сумма депозитов: {_fmt_money(st['sum'])}",
    ]
    return "\n".join(lines)


async def _show_tenant_card(c: CallbackQuery, tenant_id: int):
    async with SessionLocal() as s:
        res = await s.execute(select(Tenant).where(Tenant.id == tenant_id))
        t = res.scalar_one_or_none()
    if not t:
        await c.answer("Тенант не найден", show_alert=True)
        return
    st = await _tenant_stats(tenant_id)
    txt = _format_tenant_card(t, st)
    await c.message.edit_text(txt, reply_markup=_kb_tenant_card(t))
    await c.answer()


@router.callback_query(
    F.data.startswith("ga:t:")
    & ~F.data.contains(":start:")
    & ~F.data.contains(":pause:")
    & ~F.data.contains(":restart:")
    & ~F.data.contains(":delete")
)
async def ga_tenant_open(c: CallbackQuery):
    if not _is_ga(c.from_user.id):
        return
    tid = int(c.data.split(":")[2])
    await _show_tenant_card(c, tid)


@router.callback_query(F.data.startswith("ga:t:start:"))
async def ga_tenant_start(c: CallbackQuery):
    if not _is_ga(c.from_user.id):
        return
    tid = int(c.data.split(":")[3])
    async with SessionLocal() as s:
        await s.execute(Tenant.__table__.update().where(Tenant.id == tid).values(is_active=True))
        await s.commit()
    await _run(f"systemctl restart {CHILD_SERVICE}")
    await c.answer("Включен и перезапущены дети")
    await _show_tenant_card(c, tid)


@router.callback_query(F.data.startswith("ga:t:pause:"))
async def ga_tenant_pause(c: CallbackQuery):
    if not _is_ga(c.from_user.id):
        return
    tid = int(c.data.split(":")[3])
    async with SessionLocal() as s:
        await s.execute(Tenant.__table__.update().where(Tenant.id == tid).values(is_active=False))
        await s.commit()
    await _run(f"systemctl restart {CHILD_SERVICE}")
    await c.answer("Поставлен на паузу")
    await _show_tenant_card(c, tid)


@router.callback_query(F.data.startswith("ga:t:restart:"))
async def ga_tenant_restart(c: CallbackQuery):
    if not _is_ga(c.from_user.id):
        return
    tid = int(c.data.split(":")[3])
    await c.answer("Перезапускаю детей…")
    await _run(f"systemctl restart {CHILD_SERVICE}")
    await _show_tenant_card(c, tid)


@router.callback_query(F.data.startswith("ga:t:delete_confirm:"))
async def ga_tenant_delete_confirm(c: CallbackQuery):
    if not _is_ga(c.from_user.id):
        return
    tid = int(c.data.split(":")[3])
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="❗️ Да, удалить навсегда", callback_data=f"ga:t:delete:{tid}")],
        [InlineKeyboardButton(text="↩️ Отмена", callback_data=f"ga:t:{tid}")],
    ])
    await c.message.edit_text(
        "Вы уверены? Будут удалены сам тенант и <b>все связанные данные</b> (пользователи, события, состояния, контент).",
        reply_markup=kb
    )
    await c.answer()


@router.callback_query(F.data.startswith("ga:t:delete:"))
async def ga_tenant_delete(c: CallbackQuery):
    if not _is_ga(c.from_user.id):
        return
    tid = int(c.data.split(":")[3])

    # Полная очистка данных тенанта
    async with SessionLocal() as s:
        await s.execute(UserState.__table__.delete().where(UserState.tenant_id == tid))
        await s.execute(UserLang.__table__.delete().where(UserLang.tenant_id == tid))
        await s.execute(Event.__table__.delete().where(Event.tenant_id == tid))
        await s.execute(UserAccess.__table__.delete().where(UserAccess.tenant_id == tid))
        await s.execute(ContentOverride.__table__.delete().where(ContentOverride.tenant_id == tid))
        await s.execute(Tenant.__table__.delete().where(Tenant.id == tid))
        await s.commit()

    await _run(f"systemctl restart {CHILD_SERVICE}")
    await c.message.edit_text("🗑 Тенант и все его данные удалены. Дети перезапущены.")
    await c.answer()


# ----------------- Раннер -----------------
async def run_parent():
    bot = Bot(settings.PARENT_BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
    dp = Dispatcher()
    dp.include_router(router)
    await dp.start_polling(bot)
