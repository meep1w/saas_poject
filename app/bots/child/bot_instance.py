# app/bots/child/bot_instance.py
from __future__ import annotations

from pathlib import Path
from typing import Dict, Tuple, Optional, List

import asyncio
import hashlib
import hmac
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse

from aiogram import Bot, Dispatcher, Router, F
from aiogram.client.default import DefaultBotProperties
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from aiogram.filters import Command
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    FSInputFile,
    WebAppInfo,
)
from sqlalchemy import select, func, or_

from app.db import SessionLocal
from app.models import (
    Tenant,
    UserAccess,
    UserLang,
    UserState,
    Event,
    ContentOverride,
)
from app.settings import settings
from sqlalchemy import select, func, or_, cast, String

# =========================
#            i18n
# =========================
BASE_EN = {
    "menu_title": "Main menu",
    "lang_title": "Language",
    "lang_text": "Choose your language:",
    "btn_support": "Support",
    "btn_howto": "Instruction",
    "btn_signal": "Get signal",
    "btn_lang": "Change language",
    "btn_subscribe": "Subscribe",
    "btn_register": "Register",
    "btn_deposit": "Make a deposit",
    "btn_open_app": "Open mini-app",
    "btn_open_vip": "Open Platinum",
    "btn_check": "Check subscription",
    "back": "Back to menu",
    "howto_title": "Instruction",
    "howto_text": "1) Register via our bot.\n2) Wait for verification.\n3) Make a deposit.\n4) Tap “Get signal”.",
    "gate_sub_title": "Step 1 — Subscribe",
    "gate_sub_text": "Subscribe to the channel, then press “Get signal” again.",
    "gate_reg_title": "Step 2 — Registration",
    "gate_reg_text": "Register on PocketOption with our link, then come back.",
    "gate_dep_title": "Step 3 — Deposit",
    "gate_dep_text": "Make a deposit on PocketOption, then come back.",
    "unlocked_title": "Access unlocked 🎉",
    "unlocked_text": "Now the “Get signal” button opens the mini-app directly.",
    "platinum_title": "You are Platinum 💠",
    "platinum_text": "Advanced features are unlocked. Use the Platinum mini-app or contact support.",
}
BASE_RU = {
    "menu_title": "Главное меню",
    "lang_title": "Язык",
    "lang_text": "Выберите язык:",
    "btn_support": "Поддержка",
    "btn_howto": "Инструкция",
    "btn_signal": "Получить сигнал",
    "btn_lang": "Сменить язык",
    "btn_subscribe": "Подписаться",
    "btn_register": "Зарегистрироваться",
    "btn_deposit": "Внести депозит",
    "btn_open_app": "Открыть мини-апп",
    "btn_open_vip": "Открыть Platinum",
    "btn_check": "Проверить подписку",
    "back": "Назад в меню",
    "howto_title": "Инструкция",
    "howto_text": "1) Зарегистрируйтесь у брокера через нашего бота.\n2) Дождитесь проверки.\n3) Внесите депозит.\n4) Нажмите «Получить сигнал».",
    "gate_sub_title": "Шаг 1 — Подписка на канал",
    "gate_sub_text": "Подпишитесь на канал и вернитесь — снова нажмите «Получить сигнал».",
    "gate_reg_title": "Шаг 2 — Регистрация",
    "gate_reg_text": "Зарегистрируйтесь по нашей ссылке PocketOption. После регистрации вернитесь сюда.",
    "gate_dep_title": "Шаг 3 — Депозит",
    "gate_dep_text": "Внесите депозит на PocketOption. После зачисления вернитесь сюда.",
    "unlocked_title": "Доступ открыт 🎉",
    "unlocked_text": "Теперь кнопка «Получить сигнал» открывает мини-апп напрямую.",
    "platinum_title": "Вы — Platinum 💠",
    "platinum_text": "Расширенный функционал открыт. Откройте Platinum мини-апп или напишите в поддержку.",
}
I18N = {
    "en": BASE_EN,
    "ru": BASE_RU,
    "hi": {**BASE_EN,
           "menu_title": "मुख्य मेनू", "btn_signal": "सिग्नल प्राप्त करें",
           "btn_lang": "भाषा बदलें", "btn_subscribe": "सब्सक्राइब करें",
           "btn_register": "रजिस्टर करें", "btn_deposit": "डिपॉज़िट करें",
           "btn_open_app": "मिनी-ऐप खोलें", "btn_open_vip": "प्लैटिनम खोलें",
           "btn_check": "सब्सक्रिप्शन जांचें", "back": "मेनू पर वापस",
           "howto_title": "निर्देश",
           "howto_text": "1) हमारे बॉट से रजिस्टर करें।\n2) वेरिफिकेशन का इंतज़ार करें।\n3) डिपॉज़िट करें।\n4) “सिग्नल प्राप्त करें” दबाएँ।",
           "gate_sub_title": "कदम 1 — चैनल को सब्सक्राइब करें",
           "gate_sub_text": "पहले चैनल को सब्सक्राइब करें, फिर “सिग्नल प्राप्त करें” दबाएँ।",
           "gate_reg_title": "कदम 2 — रजिस्ट्रेशन",
           "gate_reg_text": "हमारी लिंक से PocketOption पर रजिस्टर करें, फिर वापस आएँ।",
           "gate_dep_title": "कदम 3 — डिपॉज़िट",
           "gate_dep_text": "PocketOption पर डिपॉज़िट करें, फिर वापस आएँ।",
           "unlocked_title": "एक्सेस अनलॉक 🎉",
           "unlocked_text": "अब “सिग्नल प्राप्त करें” सीधे मिनी-ऐप खोलेगा।",
           "platinum_title": "आप Platinum हैं 💠",
           "platinum_text": "एडवांस्ड फीचर्स खुल गए हैं। Platinum मिनी-ऐप खोलें या सपोर्ट से संपर्क करें.",
           },
    "es": {**BASE_EN,
           "menu_title": "Menú principal", "btn_signal": "Obtener señal",
           "btn_lang": "Cambiar idioma", "btn_subscribe": "Suscribirse",
           "btn_register": "Registrarse", "btn_deposit": "Hacer depósito",
           "btn_open_app": "Abrir mini-app", "btn_open_vip": "Abrir Platinum",
           "btn_check": "Comprobar suscripción", "back": "Volver al menú",
           "howto_title": "Instrucciones",
           "howto_text": "1) Regístrate vía nuestro bot.\n2) Espera la verificación.\n3) Haz un depósito.\n4) Pulsa “Obtener señal”.",
           "gate_sub_title": "Paso 1 — Suscribirse",
           "gate_sub_text": "Suscríbete al canal y pulsa “Obtener señal” de nuevo.",
           "gate_reg_title": "Paso 2 — Registro",
           "gate_reg_text": "Regístrate en PocketOption con nuestro enlace y vuelve.",
           "gate_dep_title": "Paso 3 — Depósito",
           "gate_dep_text": "Realiza un depósito en PocketOption y vuelve.",
           "unlocked_title": "Acceso desbloqueado 🎉",
           "unlocked_text": "Desde ahora, “Obtener señal” abre la mini-app directamente.",
           "platinum_title": "Eres Platinum 💠",
           "platinum_text": "Funciones avanzadas desbloqueadas. Abre el mini-app Platinum o contacta soporte.",
           },
}
LANGS = ["ru", "en", "hi", "es"]


# =========================
#        Assets setup
# =========================
ASSETS = {
    "menu": "menu.jpg",
    "howto": "howto.jpg",
    "lang": "language.jpg",
    "subscribe": "subscribe.jpg",
    "register": "register.jpg",
    "deposit": "deposit.jpg",
    "unlocked": "unlocked.jpg",
    "admin": "admin.jpg",          # картинка админки
    "platinum": "platinum.jpg",    # экран «Вы — Platinum»
}
ASSETS_DIR = Path("assets")


# =========================
#         Helpers
# =========================
def t(lang: str, key: str) -> str:
    base = I18N.get(lang) or I18N["en"]
    return base.get(key) or I18N["en"].get(key, key)


def asset_for(lang: str, screen: str) -> Optional[Path]:
    """
    Ищем картинку в кастомной папке языка (ru/en), затем en, затем ru (дефолтные).
    """
    candidates = []
    if lang in ("ru", "en"):
        candidates.append(ASSETS_DIR / lang / ASSETS.get(screen, "menu.jpg"))
    candidates.append(ASSETS_DIR / "en" / ASSETS.get(screen, "menu.jpg"))
    candidates.append(ASSETS_DIR / "ru" / ASSETS.get(screen, "menu.jpg"))
    for p in candidates:
        if p.exists():
            return p
    return None


def add_params(url: str, **params) -> str:
    u = urlparse(url)
    q = dict(parse_qsl(u.query))
    q.update({k: str(v) for k, v in params.items() if v is not None})
    return urlunparse(u._replace(query=urlencode(q)))


SALT = getattr(settings, "CLICK_SALT", "dev_salt_change_me")


def make_click_id(tenant_id: int, user_id: int) -> str:
    raw = f"{tenant_id}:{user_id}".encode()
    digest = hmac.new(SALT.encode(), raw, hashlib.sha256).hexdigest()[:24]
    return f"{tenant_id}-{digest}"


async def ensure_click_id(tenant_id: int, user_id: int) -> str:
    async with SessionLocal() as s:
        res = await s.execute(
            select(UserAccess).where(UserAccess.tenant_id == tenant_id, UserAccess.user_id == user_id)
        )
        ua = res.scalar_one_or_none()
        cid = (ua.click_id if ua and ua.click_id else None) or make_click_id(tenant_id, user_id)
        if not ua:
            await s.execute(
                UserAccess.__table__.insert().values(
                    tenant_id=tenant_id, user_id=user_id, click_id=cid
                )
            )
        elif not ua.click_id:
            await s.execute(
                UserAccess.__table__.update().where(UserAccess.id == ua.id).values(click_id=cid)
            )
        await s.commit()
        return cid


async def get_lang(tenant_id: int, user_id: int) -> str:
    async with SessionLocal() as s:
        res = await s.execute(
            select(UserLang).where(UserLang.tenant_id == tenant_id, UserLang.user_id == user_id)
        )
        row = res.scalar_one_or_none()
        return row.lang if row else settings.LANG_DEFAULT


async def set_lang(tenant_id: int, user_id: int, lang: str):
    async with SessionLocal() as s:
        res = await s.execute(
            select(UserLang).where(UserLang.tenant_id == tenant_id, UserLang.user_id == user_id)
        )
        row = res.scalar_one_or_none()
        if row:
            await s.execute(
                UserLang.__table__.update().where(UserLang.id == row.id).values(lang=lang)
            )
        else:
            await s.execute(
                UserLang.__table__.insert().values(tenant_id=tenant_id, user_id=user_id, lang=lang)
            )
        await s.commit()


async def get_tenant(tenant_id: int) -> Tenant:
    async with SessionLocal() as s:
        res = await s.execute(select(Tenant).where(Tenant.id == tenant_id))
        return res.scalar_one()


async def get_or_create_access(tenant_id: int, user_id: int) -> UserAccess:
    async with SessionLocal() as s:
        res = await s.execute(
            select(UserAccess).where(UserAccess.tenant_id == tenant_id, UserAccess.user_id == user_id)
        )
        acc = res.scalar_one_or_none()
        if acc is None:
            await s.execute(
                UserAccess.__table__.insert().values(
                    tenant_id=tenant_id,
                    user_id=user_id,
                    is_registered=False,
                    has_deposit=False,
                    unlocked_shown=False,
                    is_platinum=False,
                    platinum_shown=False,
                )
            )
            await s.commit()
            res = await s.execute(
                select(UserAccess).where(UserAccess.tenant_id == tenant_id, UserAccess.user_id == user_id)
            )
            acc = res.scalar_one()
        return acc


async def mark_unlocked_shown(tenant_id: int, user_id: int):
    async with SessionLocal() as s:
        await s.execute(
            UserAccess.__table__.update()
            .where(UserAccess.tenant_id == tenant_id, UserAccess.user_id == user_id)
            .values(unlocked_shown=True)
        )
        await s.commit()


async def mark_platinum_shown(tenant_id: int, user_id: int):
    async with SessionLocal() as s:
        await s.execute(
            UserAccess.__table__.update()
            .where(UserAccess.tenant_id == tenant_id, UserAccess.user_id == user_id)
            .values(platinum_shown=True)
        )
        await s.commit()


async def set_last_bot_message_id(tenant_id: int, chat_id: int, message_id: Optional[int]):
    async with SessionLocal() as s:
        res = await s.execute(
            select(UserState).where(UserState.tenant_id == tenant_id, UserState.chat_id == chat_id)
        )
        st = res.scalar_one_or_none()
        if st:
            await s.execute(
                UserState.__table__.update().where(UserState.id == st.id).values(last_bot_message_id=message_id)
            )
        else:
            await s.execute(
                UserState.__table__.insert().values(
                    tenant_id=tenant_id, chat_id=chat_id, last_bot_message_id=message_id
                )
            )
        await s.commit()


async def get_last_bot_message_id(tenant_id: int, chat_id: int) -> Optional[int]:
    async with SessionLocal() as s:
        res = await s.execute(
            select(UserState).where(UserState.tenant_id == tenant_id, UserState.chat_id == chat_id)
        )
        st = res.scalar_one_or_none()
        return st.last_bot_message_id if st else None


# -------- content overrides --------
async def resolve_title(tenant_id: int, lang: str, screen: str) -> str:
    async with SessionLocal() as s:
        r = await s.execute(
            select(ContentOverride)
            .where(ContentOverride.tenant_id == tenant_id,
                   ContentOverride.lang == lang,
                   ContentOverride.screen == screen)
        )
        ov = r.scalar_one_or_none()
        if ov and ov.title:
            return ov.title
    # fallback на i18n
    if screen == "menu":
        return t(lang, "menu_title")
    if screen == "howto":
        return t(lang, "howto_title")
    if screen == "unlocked":
        return t(lang, "unlocked_title")
    if screen == "platinum":
        return t(lang, "platinum_title")
    if screen == "register":
        return t(lang, "gate_reg_title")
    if screen == "deposit":
        return t(lang, "gate_dep_title")
    if screen == "subscribe":
        return t(lang, "gate_sub_title")
    if screen == "admin":
        return "Панель администратора"
    return screen


async def resolve_primary_btn_text(tenant_id: int, lang: str, screen: str) -> Optional[str]:
    async with SessionLocal() as s:
        r = await s.execute(
            select(ContentOverride)
            .where(ContentOverride.tenant_id == tenant_id,
                   ContentOverride.lang == lang,
                   ContentOverride.screen == screen)
        )
        ov = r.scalar_one_or_none()
        if ov and ov.primary_btn_text:
            return ov.primary_btn_text
    if screen == "menu":
        return t(lang, "btn_signal")
    if screen == "howto":
        return t(lang, "btn_open_app")
    return None


async def resolve_image(tenant_id: int, lang: str, screen: str) -> Optional[str]:
    async with SessionLocal() as s:
        r = await s.execute(
            select(ContentOverride)
            .where(ContentOverride.tenant_id == tenant_id,
                   ContentOverride.lang == lang,
                   ContentOverride.screen == screen)
        )
        ov = r.scalar_one_or_none()
        if ov and ov.image_path:
            return ov.image_path  # может быть file_id Telegram
    return None


async def upsert_override(
    tenant_id: int,
    lang: str,
    screen: str,
    title: Optional[str] = None,
    primary_btn_text: Optional[str] = None,
    image_path: Optional[str] = None,
    reset: bool = False,
):
    async with SessionLocal() as s:
        r = await s.execute(
            select(ContentOverride)
            .where(ContentOverride.tenant_id == tenant_id,
                   ContentOverride.lang == lang,
                   ContentOverride.screen == screen)
        )
        ov = r.scalar_one_or_none()
        if reset:
            if ov:
                await s.delete(ov)
            await s.commit()
            return

        vals = {}
        if title is not None:
            vals["title"] = title
        if primary_btn_text is not None:
            vals["primary_btn_text"] = primary_btn_text
        if image_path is not None:
            vals["image_path"] = image_path

        if ov:
            await s.execute(
                ContentOverride.__table__.update().where(ContentOverride.id == ov.id).values(**vals)
            )
        else:
            await s.execute(
                ContentOverride.__table__.insert().values(
                    tenant_id=tenant_id, lang=lang, screen=screen, **vals
                )
            )
        await s.commit()


# -------- common send --------
async def send_screen(
    bot: Bot,
    tenant_id: int,
    chat_id: int,
    lang: str,
    screen: str,
    text: str,
    kb: Optional[InlineKeyboardMarkup],
):
    last_id = await get_last_bot_message_id(tenant_id, chat_id)
    if last_id:
        try:
            await bot.delete_message(chat_id, last_id)
        except (TelegramBadRequest, TelegramForbiddenError):
            pass

    # Кастомная картинка (в том числе file_id), затем дефолт из assets
    custom = await resolve_image(tenant_id, lang, screen)
    photo = None
    if custom:
        p = Path(custom)
        photo = FSInputFile(str(p)) if p.exists() else custom  # если строка — считаем file_id
    if photo:
        msg = await bot.send_photo(chat_id, photo=photo, caption=text, reply_markup=kb)
    else:
        p = asset_for(lang, screen)
        if p and p.exists():
            msg = await bot.send_photo(chat_id, photo=FSInputFile(str(p)), caption=text, reply_markup=kb)
        else:
            msg = await bot.send_message(chat_id, text=text, reply_markup=kb, disable_web_page_preview=True)

    await set_last_bot_message_id(tenant_id, chat_id, msg.message_id)


# -------- metrics --------
async def user_deposit_sum(tid: int, click_id: str) -> float:
    async with SessionLocal() as s:
        val = (await s.execute(
            select(func.coalesce(func.sum(Event.amount), 0.0)).where(
                Event.tenant_id == tid, Event.click_id == click_id, Event.kind.in_(("ftd", "rd"))
            )
        )).scalar()
        return float(val or 0.0)


# =========================
#         Keyboards
# =========================
def kb_subscribe(lang: str, ch_url: Optional[str]) -> InlineKeyboardMarkup:
    url = ch_url or settings.CHANNEL_URL
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=t(lang, "btn_subscribe"), url=url)],
            [InlineKeyboardButton(text=t(lang, "btn_check"), callback_data="check_sub")],
            [InlineKeyboardButton(text=t(lang, "back"), callback_data="menu")],
        ]
    )


def kb_register(lang: str, url: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=t(lang, "btn_register"), url=url)],
            [InlineKeyboardButton(text=t(lang, "back"), callback_data="menu")],
        ]
    )


def kb_deposit(lang: str, url: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=t(lang, "btn_deposit"), url=url)],
            [InlineKeyboardButton(text=t(lang, "back"), callback_data="menu")],
        ]
    )


def kb_open_app(lang: str, support_url: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=t(lang, "btn_open_app"), web_app=WebAppInfo(url=settings.MINIAPP_URL))],
            [InlineKeyboardButton(text=t(lang, "btn_support"), url=support_url)],
            [InlineKeyboardButton(text=t(lang, "back"), callback_data="menu")],
        ]
    )


def kb_open_platinum(lang: str, support_url: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=t(lang, "btn_open_vip"), web_app=WebAppInfo(url=settings.PLATINUM_MINIAPP_URL))],
            [InlineKeyboardButton(text=t(lang, "btn_support"), url=support_url)],
            [InlineKeyboardButton(text=t(lang, "back"), callback_data="menu")],
        ]
    )


def main_kb(lang: str, acc: UserAccess, support_url: str, menu_btn_text: Optional[str] = None) -> InlineKeyboardMarkup:
    """
    Лэйаут:
      1) Инструкция (широкая)
      2) Поддержка | Сменить язык (2 колонки)
      3) Нижняя широкая: "Открыть Platinum" или "Получить сигнал"
    """
    direct = acc.has_deposit or acc.is_platinum
    signal_text = menu_btn_text or t(lang, "btn_signal")

    rows: list[list[InlineKeyboardButton]] = []

    # 1) Инструкция
    rows.append([InlineKeyboardButton(text=t(lang, "btn_howto"), callback_data="howto")])

    # 2) Поддержка | Сменить язык
    rows.append([
        InlineKeyboardButton(text=t(lang, "btn_support"), url=support_url),
        InlineKeyboardButton(text=t(lang, "btn_lang"), callback_data="lang"),
    ])

    # 3) Нижняя широкая
    if acc.is_platinum:
        rows.append([
            InlineKeyboardButton(text=t(lang, "btn_open_vip"), web_app=WebAppInfo(url=settings.PLATINUM_MINIAPP_URL))
        ])
    else:
        if direct:
            rows.append([
                InlineKeyboardButton(text=signal_text, web_app=WebAppInfo(url=settings.MINIAPP_URL))
            ])
        else:
            rows.append([InlineKeyboardButton(text=signal_text, callback_data="signal")])

    return InlineKeyboardMarkup(inline_keyboard=rows)


def build_lang_kb(current: str) -> InlineKeyboardMarkup:
    row, rows = [], []
    for code in LANGS:
        mark = "✅ " if code == current else ""
        row.append(InlineKeyboardButton(text=f"{mark}{code.upper()}", callback_data=f"set_lang:{code}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton(text=t(current, "back"), callback_data="menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


# =========================
#        Signal flow
# =========================
async def check_membership(bot: Bot, channel_id: Optional[int], user_id: int) -> bool:
    if not channel_id:
        return False
    try:
        member = await bot.get_chat_member(channel_id, user_id)
        return getattr(member, "status", None) in {"creator", "administrator", "member"}
    except Exception:
        return False


async def _auto_check_after_subscribe(bot: Bot, tenant_id: int, user_id: int, chat_id: int, lang: str):
    await asyncio.sleep(12)
    if await check_membership(bot, (await get_tenant(tenant_id)).gate_channel_id, user_id):
        await route_signal(bot, tenant_id, user_id, chat_id, lang)


async def route_signal(bot: Bot, tenant_id: int, user_id: int, chat_id: int, lang: str):
    async with SessionLocal() as s:
        t_res = await s.execute(select(Tenant).where(Tenant.id == tenant_id))
        tenant = t_res.scalar_one()
        a_res = await s.execute(
            select(UserAccess).where(UserAccess.tenant_id == tenant_id, UserAccess.user_id == user_id)
        )
        access = a_res.scalar_one_or_none()
        if access is None:
            await s.execute(
                UserAccess.__table__.insert().values(
                    tenant_id=tenant_id,
                    user_id=user_id,
                    is_registered=False,
                    has_deposit=False,
                    unlocked_shown=False,
                    is_platinum=False,
                    platinum_shown=False,
                )
            )
            await s.commit()
            a_res = await s.execute(
                select(UserAccess).where(UserAccess.tenant_id == tenant_id, UserAccess.user_id == user_id)
            )
            access = a_res.scalar_one()

    support_url = tenant.support_url or settings.SUPPORT_URL

    # 1) Подписка (если включена)
    if tenant.check_subscription:
        if not await check_membership(bot, tenant.gate_channel_id, user_id):
            text = f"<b>{t(lang, 'gate_sub_title')}</b>\n\n{t(lang, 'gate_sub_text')}"
            await send_screen(bot, tenant_id, chat_id, lang, "subscribe", text, kb_subscribe(lang, tenant.gate_channel_url))
            asyncio.create_task(_auto_check_after_subscribe(bot, tenant_id, user_id, chat_id, lang))
            return

    # 2) Регистрация (обязательна)
    ref = tenant.ref_link or settings.REF_LINK
    cid = await ensure_click_id(tenant_id, user_id)
    ref_url = add_params(ref, click_id=cid, tid=tenant_id)
    if not access.is_registered:
        text = f"<b>{t(lang, 'gate_reg_title')}</b>\n\n{t(lang, 'gate_reg_text')}"
        await send_screen(bot, tenant_id, chat_id, lang, "register", text, kb_register(lang, ref_url))
        return

    # 3) Депозит (если включён и не достигнут минимум)
    if tenant.check_deposit:
        dep = tenant.deposit_link or settings.DEPOSIT_LINK
        dep_url = add_params(dep, click_id=cid, tid=tenant_id)
        total = await user_deposit_sum(tenant_id, cid)
        need = float(tenant.min_deposit_usd or 0.0)
        if total < need:
            text = f"<b>{t(lang, 'gate_dep_title')}</b>\n\n{t(lang, 'gate_dep_text')}"
            await send_screen(bot, tenant_id, chat_id, lang, "deposit", text, kb_deposit(lang, dep_url))
            return

    # 4) Platinum уведомление
    if access.is_platinum and not access.platinum_shown:
        text = f"<b>{t(lang, 'platinum_title')}</b>\n\n{t(lang, 'platinum_text')}"
        await send_screen(bot, tenant_id, chat_id, lang, "platinum", text, kb_open_platinum(lang, support_url))
        await mark_platinum_shown(tenant_id, user_id)
        return

    # 5) “Доступ открыт” — если ещё не показали
    if not access.unlocked_shown:
        text = f"<b>{t(lang, 'unlocked_title')}</b>\n\n{t(lang, 'unlocked_text')}"
        await send_screen(bot, tenant_id, chat_id, lang, "unlocked", text, kb_open_app(lang, support_url))
        await mark_unlocked_shown(tenant_id, user_id)
        return

    # 6) Меню (Platinum открывает VIP мини-апп из кнопки)
    title = await resolve_title(tenant_id, lang, "menu")
    btn_text = await resolve_primary_btn_text(tenant_id, lang, "menu")
    await send_screen(
        bot, tenant_id, chat_id, lang, "menu", title,
        main_kb(lang, access, support_url, btn_text)
    )


# =========================
#        Admin section
# =========================
ADMIN_WAIT: Dict[Tuple[int, int], str] = {}
BROADCAST_STATE: Dict[Tuple[int, int], dict] = {}  # (tenant, owner) -> {"segment","text","photo_id"}

PAGE_SIZE = 8


def kb_admin_main() -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text="👤 Пользователи", callback_data="adm:users:0")],
        [InlineKeyboardButton(text="🧷 Настройка постбэков", callback_data="adm:pb")],
        [InlineKeyboardButton(text="🧩 Контент", callback_data="adm:content"),
         InlineKeyboardButton(text="🔗 Ссылки", callback_data="adm:links")],
        [InlineKeyboardButton(text="⚙️ Параметры", callback_data="adm:params"),
         InlineKeyboardButton(text="📣 Рассылка", callback_data="adm:bc")],
        [InlineKeyboardButton(text="📊 Статистика", callback_data="adm:stats")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


def kb_users_list(items: list[UserAccess], page: int, more: bool) -> InlineKeyboardMarkup:
    rows = []
    # строка поиска сверху
    rows.append([InlineKeyboardButton(text="🔎 Поиск", callback_data="adm:users:search")])

    for ua in items:
        mark_r = "✅" if ua.is_registered else "❌"
        mark_d = "✅" if ua.has_deposit else "❌"
        mark_p = "💠" if ua.is_platinum else "•"
        rows.append([InlineKeyboardButton(
            text=f"{ua.user_id}  R:{mark_r}  D:{mark_d}  {mark_p}",
            callback_data=f"adm:user:{ua.user_id}"
        )])
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="⬅️ Назад", callback_data=f"adm:users:{page-1}"))
    if more:
        nav.append(InlineKeyboardButton(text="Вперёд ➡️", callback_data=f"adm:users:{page+1}"))
    if nav:
        rows.append(nav)
    rows.append([InlineKeyboardButton(text="↩️ В меню", callback_data="adm:menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def kb_user_card(ua: UserAccess) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text=("Выдать регистрацию ✅" if not ua.is_registered else "Снять регистрацию ❌"),
                              callback_data=f"adm:user:toggle_reg:{ua.user_id}")],
        [InlineKeyboardButton(text=("Выдать депозит ✅" if not ua.has_deposit else "Снять депозит ❌"),
                              callback_data=f"adm:user:toggle_dep:{ua.user_id}")],
        [InlineKeyboardButton(text=("Выдать Platinum 💠" if not ua.is_platinum else "Снять Platinum •"),
                              callback_data=f"adm:user:toggle_plat:{ua.user_id}")],
        [InlineKeyboardButton(text="↩️ К списку", callback_data="adm:users:0"),
         InlineKeyboardButton(text="↩️ В меню", callback_data="adm:menu")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


def kb_links() -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text="Изменить реф-ссылку", callback_data="adm:links:set:ref")],
        [InlineKeyboardButton(text="Изменить ссылку депоз.", callback_data="adm:links:set:dep")],
        [InlineKeyboardButton(text="Изменить канал (ID → URL)", callback_data="adm:links:set:chan")],
        [InlineKeyboardButton(text="Изменить Support URL", callback_data="adm:links:set:support")],
        [InlineKeyboardButton(text="Задать PB Secret", callback_data="adm:links:set:pbsec")],
        [InlineKeyboardButton(text="↩️ В меню", callback_data="adm:menu")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


def kb_postbacks(tenant_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="↩️ В меню", callback_data="adm:menu")]
    ])


def kb_bc_menu(state: dict) -> InlineKeyboardMarkup:
    seg = state.get("segment", "all")
    rows = [
        [InlineKeyboardButton(text=("✅ Всем" if seg == "all" else "Всем"), callback_data="adm:bc:seg:all"),
         InlineKeyboardButton(text=("✅ С регистрацией" if seg == "reg" else "С регистрацией"), callback_data="adm:bc:seg:reg")],
        [InlineKeyboardButton(text=("✅ С депозитом" if seg == "dep" else "С депозитом"), callback_data="adm:bc:seg:dep"),
         InlineKeyboardButton(text=("✅ Только /start" if seg == "nosteps" else "Только /start"), callback_data="adm:bc:seg:nosteps")],
        [InlineKeyboardButton(text="📝 Задать текст", callback_data="adm:bc:text"),
         InlineKeyboardButton(text="🖼️ Прикрепить фото", callback_data="adm:bc:photo")],
        [InlineKeyboardButton(text="🚀 Запустить", callback_data="adm:bc:run")],
        [InlineKeyboardButton(text="↩️ В меню", callback_data="adm:menu")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


# ---- Admin handlers
def make_child_router(tenant_id: int) -> Router:
    router = Router()

    # ---- public
    @router.message(Command("start"))
    async def on_start(m: Message):
        lang = await get_lang(tenant_id, m.from_user.id)
        acc = await get_or_create_access(tenant_id, m.from_user.id)
        # сохраняем текущий username (если есть)
        if m.from_user.username:
            async with SessionLocal() as s:
                await s.execute(
                    UserAccess.__table__.update()
                    .where(
                        UserAccess.tenant_id == tenant_id,
                        UserAccess.user_id == m.from_user.id
                    )
                    .values(username=m.from_user.username)
                )
                await s.commit()

        tnt = await get_tenant(tenant_id)
        sup = tnt.support_url or settings.SUPPORT_URL
        menu_btn = await resolve_primary_btn_text(tenant_id, lang, "menu")
        title = await resolve_title(tenant_id, lang, "menu")
        await send_screen(
            m.bot, tenant_id, m.chat.id, lang, "menu", title,
            main_kb(lang, acc, sup, menu_btn)
        )

    @router.message(Command("my_click"))
    async def my_click(m: Message):
        cid = await ensure_click_id(tenant_id, m.from_user.id)
        await m.answer(f"Ваш click_id:\n<code>{cid}</code>")

    @router.callback_query(F.data == "menu")
    async def cb_menu(c: CallbackQuery):
        lang = await get_lang(tenant_id, c.from_user.id)
        acc = await get_or_create_access(tenant_id, c.from_user.id)
        tnt = await get_tenant(tenant_id)
        sup = tnt.support_url or settings.SUPPORT_URL
        menu_btn = await resolve_primary_btn_text(tenant_id, lang, "menu")
        title = await resolve_title(tenant_id, lang, "menu")
        await send_screen(
            c.bot, tenant_id, c.message.chat.id, lang, "menu", title,
            main_kb(lang, acc, sup, menu_btn)
        )
        await c.answer()

    @router.callback_query(F.data == "howto")
    async def cb_howto(c: CallbackQuery):
        lang = await get_lang(tenant_id, c.from_user.id)
        tnt = await get_tenant(tenant_id)
        sup = tnt.support_url or settings.SUPPORT_URL
        title = await resolve_title(tenant_id, lang, "howto")
        await send_screen(c.bot, tenant_id, c.message.chat.id, lang, "howto", title, kb_open_app(lang, sup))
        await c.answer()

    @router.callback_query(F.data == "signal")
    async def cb_signal(c: CallbackQuery):
        lang = await get_lang(tenant_id, c.from_user.id)
        await route_signal(c.bot, tenant_id, c.from_user.id, c.message.chat.id, lang)
        await c.answer()

    @router.callback_query(F.data == "check_sub")
    async def cb_check_sub(c: CallbackQuery):
        lang = await get_lang(tenant_id, c.from_user.id)
        await route_signal(c.bot, tenant_id, c.from_user.id, c.message.chat.id, lang)
        await c.answer()

    @router.callback_query(F.data == "lang")
    async def cb_lang(c: CallbackQuery):
        lang = await get_lang(tenant_id, c.from_user.id)
        text = f"<b>{t(lang, 'lang_title')}</b>\n\n{t(lang, 'lang_text')}"
        await send_screen(c.bot, tenant_id, c.message.chat.id, lang, "lang", text, build_lang_kb(lang))
        await c.answer()

    @router.callback_query(F.data.startswith("set_lang:"))
    async def cb_set_lang(c: CallbackQuery):
        new_lang = c.data.split(":", 1)[1]
        if new_lang not in LANGS:
            await c.answer("Unsupported lang", show_alert=True)
            return
        await set_lang(tenant_id, c.from_user.id, new_lang)
        acc = await get_or_create_access(tenant_id, c.from_user.id)
        tnt = await get_tenant(tenant_id)
        sup = tnt.support_url or settings.SUPPORT_URL
        title = await resolve_title(tenant_id, new_lang, "menu")
        menu_btn = await resolve_primary_btn_text(tenant_id, new_lang, "menu")
        await send_screen(
            c.bot, tenant_id, c.message.chat.id, new_lang, "menu", title,
            main_kb(new_lang, acc, sup, menu_btn)
        )
        await c.answer()

    # ---- admin gate
    async def is_owner(tenant_id_: int, user_id_: int) -> bool:
        tnt = await get_tenant(tenant_id_)
        return tnt.owner_telegram_id == user_id_


    @router.message(Command("admin"))
    async def on_admin(m: Message):
        if not await is_owner(tenant_id, m.from_user.id):
            await m.answer("Доступ запрещён.")
            return
        title = await resolve_title(tenant_id, "ru", "admin")
        await send_screen(m.bot, tenant_id, m.chat.id, "ru", "admin", title, kb_admin_main())

    @router.callback_query(F.data == "adm:users:search")
    async def adm_users_search(c: CallbackQuery):
        if not await is_owner(tenant_id, c.from_user.id):
            return
        ADMIN_WAIT[(tenant_id, c.from_user.id)] = "users_search"
        await c.message.answer("Введите TG ID, trader_id или часть click_id.")
        await c.answer()

    @router.callback_query(F.data == "adm:menu")
    async def adm_menu(c: CallbackQuery):
        if not await is_owner(tenant_id, c.from_user.id): return
        title = await resolve_title(tenant_id, "ru", "admin")
        await send_screen(c.bot, tenant_id, c.message.chat.id, "ru", "admin", title, kb_admin_main())
        await c.answer()

    # ---- Users
    async def _fetch_users_page(tid: int, page: int):
        async with SessionLocal() as s:
            total = (await s.execute(
                select(func.count()).select_from(UserAccess).where(UserAccess.tenant_id == tid)
            )).scalar()
            res = await s.execute(
                select(UserAccess).where(UserAccess.tenant_id == tid)
                .order_by(UserAccess.id.desc()).offset(page * PAGE_SIZE).limit(PAGE_SIZE)
            )
            items = res.scalars().all()
        more = (page + 1) * PAGE_SIZE < (total or 0)
        return items, more, total

    async def _search_users(bot: Bot, tid: int, q: str) -> List[UserAccess]:
        q = q.strip()
        async with SessionLocal() as s:
            # числа — как TG ID
            if q.isdigit():
                res = await s.execute(
                    select(UserAccess).where(UserAccess.tenant_id == tid, UserAccess.user_id == int(q))
                )
                return res.scalars().all()

            # @username — по лучшему предположению: берём последних 200 и сравниваем username
            if q.startswith("@"):
                res = await s.execute(
                    select(UserAccess).where(UserAccess.tenant_id == tid).order_by(UserAccess.id.desc()).limit(200)
                )
                found = []
                for ua in res.scalars().all():
                    try:
                        ch = await bot.get_chat(ua.user_id)
                        if ch.username and f"@{ch.username.lower()}" == q.lower():
                            found.append(ua)
                    except Exception:
                        pass
                return found

            # иначе ищем по trader_id / click_id (LIKE)
            res = await s.execute(
                select(UserAccess).where(
                    UserAccess.tenant_id == tid,
                    or_(
                        UserAccess.trader_id.ilike(f"%{q}%"),
                        UserAccess.click_id.ilike(f"%{q}%"),
                    )
                ).order_by(UserAccess.id.desc()).limit(PAGE_SIZE)
            )
            return res.scalars().all()

    @router.callback_query(F.data.startswith("adm:users:"))
    async def adm_users(c: CallbackQuery):
        if not await is_owner(tenant_id, c.from_user.id): return
        tail = c.data.split(":")[2]
        if tail == "search":
            ADMIN_WAIT[(tenant_id, c.from_user.id)] = "users_search"
            await c.message.answer("Введите TG ID, @username, trader_id или часть click_id.")
            await c.answer()
            return
        page = int(tail)
        items, more, total = await _fetch_users_page(tenant_id, page)
        txt = f"👤 Пользователи ({total or 0})\n\nВыберите пользователя:"
        await send_screen(c.bot, tenant_id, c.message.chat.id, "ru", "admin", txt, kb_users_list(items, page, more))
        await c.answer()

    async def _user_deposit_sum(tid: int, click_id: str) -> float:
        return await user_deposit_sum(tid, click_id)

    @router.callback_query(F.data.startswith("adm:user:") & ~F.data.startswith("adm:user:toggle"))
    async def adm_user_card(c: CallbackQuery):
        if not await is_owner(tenant_id, c.from_user.id): return
        uid = int(c.data.split(":")[2])
        async with SessionLocal() as s:
            res = await s.execute(
                select(UserAccess).where(UserAccess.tenant_id == tenant_id, UserAccess.user_id == uid))
            ua = res.scalar_one_or_none()
        if not ua:
            await c.answer("Пользователь не найден", show_alert=True)
            return
        dep_sum = await _user_deposit_sum(tenant_id, ua.click_id or "")
        lang = await get_lang(tenant_id, uid)
        text = (
            f"🧾 Карточка пользователя\n\n"
            f"TG ID: <code>{ua.user_id}</code>\n"
            f"Язык: {lang}\n"
            f"Click ID: <code>{ua.click_id or '-'}</code>\n"
            f"Trader ID: <code>{ua.trader_id or '-'}</code>\n"
            f"Регистрация: {'✅' if ua.is_registered else '❌'}\n"
            f"Депозит (факт): {'✅' if ua.has_deposit else '❌'}\n"
            f"Сумма депозитов: {dep_sum:.2f}\n"
            f"Platinum: {'💠' if ua.is_platinum else '•'}\n"
            f"Создан: {ua.created_at:%Y-%m-%d %H:%M}\n"
        )
        await send_screen(c.bot, tenant_id, c.message.chat.id, "ru", "admin", text, kb_user_card(ua))
        await c.answer()

    @router.callback_query(F.data.startswith("adm:user:toggle_reg:"))
    async def adm_user_toggle_reg(c: CallbackQuery):
        if not await is_owner(tenant_id, c.from_user.id): return
        uid = int(c.data.rsplit(":", 1)[1])
        async with SessionLocal() as s:
            res = await s.execute(
                select(UserAccess).where(UserAccess.tenant_id == tenant_id, UserAccess.user_id == uid))
            ua = res.scalar_one_or_none()
            if not ua: await c.answer("Не найден", show_alert=True); return
            newv = not ua.is_registered
            await s.execute(UserAccess.__table__.update().where(UserAccess.id == ua.id).values(is_registered=newv))
            await s.commit()
        await c.answer("Готово")
        await adm_user_card(c)

    @router.callback_query(F.data.startswith("adm:user:toggle_dep:"))
    async def adm_user_toggle_dep(c: CallbackQuery):
        if not await is_owner(tenant_id, c.from_user.id): return
        uid = int(c.data.rsplit(":", 1)[1])
        async with SessionLocal() as s:
            res = await s.execute(
                select(UserAccess).where(UserAccess.tenant_id == tenant_id, UserAccess.user_id == uid))
            ua = res.scalar_one_or_none()
            if not ua: await c.answer("Не найден", show_alert=True); return
            newv = not ua.has_deposit
            await s.execute(UserAccess.__table__.update().where(UserAccess.id == ua.id).values(has_deposit=newv))
            await s.commit()
        await c.answer("Готово")
        await adm_user_card(c)

    @router.callback_query(F.data.startswith("adm:user:toggle_plat:"))
    async def adm_user_toggle_plat(c: CallbackQuery):
        if not await is_owner(tenant_id, c.from_user.id): return
        uid = int(c.data.rsplit(":", 1)[1])
        async with SessionLocal() as s:
            res = await s.execute(
                select(UserAccess).where(UserAccess.tenant_id == tenant_id, UserAccess.user_id == uid))
            ua = res.scalar_one_or_none()
            if not ua: await c.answer("Не найден", show_alert=True); return
            newv = not ua.is_platinum
            await s.execute(UserAccess.__table__.update().where(UserAccess.id == ua.id).values(is_platinum=newv))
            await s.commit()
        await c.answer("Готово")
        await adm_user_card(c)

    # ---- Postbacks config
    def _postbacks_text(tid: int, secret: Optional[str]) -> str:
        base = settings.POSTBACK_BASE.rstrip("/")
        sec = f"&secret={secret}" if secret else ""
        return (
            "🧷 Настройка постбэков\n\n"
            "Вставьте эти URL в кабинете партнёрки (PocketPartners).\n"
            "Обязательно включите макросы: {click_id}, {trader_id}, {sumdep}.\n\n"
            "<b>Регистрация</b>\n"
            f"<code>{base}/pp/reg?click_id={{click_id}}&trader_id={{trader_id}}&tid={tid}{sec}</code>\n\n"
            "<b>Первый депозит</b>\n"
            f"<code>{base}/pp/ftd?click_id={{click_id}}&sumdep={{sumdep}}&trader_id={{trader_id}}&tid={tid}{sec}</code>\n\n"
            "<b>Повторный депозит</b>\n"
            f"<code>{base}/pp/rd?click_id={{click_id}}&sumdep={{sumdep}}&tid={tid}{sec}</code>\n"
        )

    @router.callback_query(F.data == "adm:pb")
    async def adm_postbacks(c: CallbackQuery):
        if not await is_owner(tenant_id, c.from_user.id): return
        tnt = await get_tenant(tenant_id)
        txt = _postbacks_text(tenant_id, tnt.pb_secret)
        await send_screen(c.bot, tenant_id, c.message.chat.id, "ru", "admin", txt, kb_postbacks(tenant_id))
        await c.answer()

    # ---- Links
    @router.callback_query(F.data == "adm:links")
    async def adm_links(c: CallbackQuery):
        if not await is_owner(tenant_id, c.from_user.id): return
        tnt = await get_tenant(tenant_id)
        text = ("🔗 Ссылки\n\n"
                f"Ref: {tnt.ref_link or '—'}\n"
                f"Deposit: {tnt.deposit_link or '—'}\n"
                f"Channel ID: {tnt.gate_channel_id or '—'}\n"
                f"Channel URL: {tnt.gate_channel_url or '—'}\n"
                f"Support URL: {tnt.support_url or settings.SUPPORT_URL}\n"
                f"PB Secret: {tnt.pb_secret or '—'}\n\n"
                "Выберите, что изменить.")
        await send_screen(c.bot, tenant_id, c.message.chat.id, "ru", "admin", text, kb_links())
        await c.answer()

    @router.callback_query(F.data.startswith("adm:links:set:"))
    async def adm_links_set(c: CallbackQuery):
        if not await is_owner(tenant_id, c.from_user.id): return
        action = c.data.split(":")[-1]
        key = (tenant_id, c.from_user.id)
        if action == "ref":
            ADMIN_WAIT[key] = "/set_ref_link";  await c.message.answer("Пришлите новую <b>реф-ссылку</b> (https://...)", parse_mode="HTML")
        elif action == "dep":
            ADMIN_WAIT[key] = "/set_deposit_link";  await c.message.answer("Пришлите новую <b>ссылку депозита</b> (https://...)", parse_mode="HTML")
        elif action == "chan":
            ADMIN_WAIT[key] = "/set_channel_id";  await c.message.answer("Пришлите <b>ID канала</b> вида -1001234567890", parse_mode="HTML")
        elif action == "support":
            ADMIN_WAIT[key] = "/set_support_url"; await c.message.answer("Пришлите новый <b>Support URL</b> (https://...)", parse_mode="HTML")
        elif action == "pbsec":
            ADMIN_WAIT[key] = "/set_pb_secret";   await c.message.answer("Пришлите новый секрет (латиница/цифры), можно «-» чтобы очистить.", parse_mode="HTML")
        await c.answer()

    # команды-сеттеры
    @router.message(F.text.regexp(r"^/set_channel_id\s+(-?\d{5,})$"))
    async def set_channel_id_cmd(m: Message):
        tnt_ok = await is_owner(tenant_id, m.from_user.id)
        if not tnt_ok: return
        ch_id = int(m.text.split()[1])
        async with SessionLocal() as s:
            await s.execute(Tenant.__table__.update().where(Tenant.id == tenant_id).values(gate_channel_id=ch_id))
            await s.commit()
        await m.answer(f"gate_channel_id сохранён: {ch_id}")

    # ловим URL’ы
    @router.message(F.text.regexp(r"^https?://\S+$"))
    async def admin_catch_url(m: Message):
        key = (tenant_id, m.from_user.id)
        cmd = ADMIN_WAIT.get(key)
        if not cmd:
            return
        url = m.text.strip()
        col_map = {
            "/set_channel_url": "gate_channel_url",
            "/set_ref_link": "ref_link",
            "/set_deposit_link": "deposit_link",
            "/set_support_url": "support_url",
        }
        col = col_map.get(cmd)
        if not col:
            return
        async with SessionLocal() as s:
            await s.execute(Tenant.__table__.update().where(Tenant.id == tenant_id).values(**{col: url}))
            await s.commit()
        ADMIN_WAIT.pop(key, None)
        await m.answer(f"{col} сохранён: {url}")

    @router.message(F.text.regexp(r"^-?\d{5,}$"))
    async def admin_catch_id(m: Message):
        key = (tenant_id, m.from_user.id)
        cmd = ADMIN_WAIT.get(key)
        if cmd != "/set_channel_id":
            return
        ch_id = int(m.text.strip())
        async with SessionLocal() as s:
            await s.execute(Tenant.__table__.update().where(Tenant.id == tenant_id).values(gate_channel_id=ch_id))
            await s.commit()
        ADMIN_WAIT.pop(key, None)
        await m.answer(f"gate_channel_id сохранён: {ch_id}")

    @router.message(F.text)
    async def catch_admin_text(m: Message):
        key = (tenant_id, m.from_user.id)
        wait = ADMIN_WAIT.get(key)
        if not wait:
            return

        # поиск пользователей (админ)
        if wait == "users_search":
            query_raw = m.text.strip()
            ADMIN_WAIT.pop(key, None)

            like = f"%{query_raw}%"

            async with SessionLocal() as s:
                # 1) прямой поиск в user_access
                conds = [
                    cast(UserAccess.user_id, String).ilike(like),
                    UserAccess.click_id.ilike(like),
                    cast(UserAccess.trader_id, String).ilike(like),
                ]
                # TG ID — точное совпадение тоже учитываем
                if query_raw.isdigit():
                    conds.append(UserAccess.user_id == int(query_raw))
                # username (если сохранился в /start)
                conds.append(cast(UserAccess.username, String).ilike(like))

                res = await s.execute(
                    select(UserAccess)
                    .where(
                        UserAccess.tenant_id == tenant_id,
                        or_(*conds)
                    )
                    .order_by(UserAccess.id.desc())
                    .limit(50)
                )
                items = res.scalars().all()

                # 2) fallback через events: ищем trader_id/click_id в событиях
                if not items:
                    ev_rows = await s.execute(
                        select(Event.click_id)
                        .where(
                            Event.tenant_id == tenant_id,
                            or_(
                                cast(Event.trader_id, String).ilike(like),
                                Event.click_id.ilike(like),
                            )
                        )
                        .limit(200)
                    )
                    click_ids = [r[0] for r in ev_rows.all()]
                    if click_ids:
                        res = await s.execute(
                            select(UserAccess)
                            .where(
                                UserAccess.tenant_id == tenant_id,
                                UserAccess.click_id.in_(click_ids)
                            )
                            .order_by(UserAccess.id.desc())
                        )
                        items = res.scalars().all()

            txt = f"🔎 Результаты поиска: {len(items)}"
            kb = kb_users_list(items, page=0, more=False)
            # экран админки, чтобы была админ-картинка
            await send_screen(m.bot, tenant_id, m.chat.id, "ru", "admin", txt, kb)
            return

        # рассылка: текст
        if wait == "bc_text":
            state = BROADCAST_STATE.setdefault(key, {"segment": "all"})
            state["text"] = m.text.strip()
            ADMIN_WAIT.pop(key, None)
            await m.answer("Текст сохранён ✅")
            fake_cb = CallbackQuery(id="0", from_user=m.from_user, message=m, data="adm:bc")
            await adm_bc(fake_cb)  # type: ignore[arg-type]
            return

        # контент: заголовок
        if wait.startswith("content_title:"):
            _, lang, screen = wait.split(":")
            await upsert_override(tenant_id, lang, screen, title=m.text.strip())
            ADMIN_WAIT.pop(key, None)
            await m.answer("Заголовок сохранён ✅")
            fake_cb = CallbackQuery(id="0", from_user=m.from_user, message=m, data=f"adm:content:edit:{lang}:{screen}")
            await adm_content_edit(fake_cb)  # type: ignore[arg-type]
            return

        # контент: текст кнопки
        if wait.startswith("content_btn:"):
            _, lang, screen = wait.split(":")
            await upsert_override(tenant_id, lang, screen, primary_btn_text=m.text.strip())
            ADMIN_WAIT.pop(key, None)
            await m.answer("Текст кнопки сохранён ✅")
            fake_cb = CallbackQuery(id="0", from_user=m.from_user, message=m, data=f"adm:content:edit:{lang}:{screen}")
            await adm_content_edit(fake_cb)  # type: ignore[arg-type]
            return



        # параметры: числа
        if wait in ("param:min_dep", "param:plat"):
            txt = m.text.strip().replace(",", ".")
            try:
                val = float(txt)
            except ValueError:
                await m.answer("Нужно число. Попробуйте ещё раз.")
                return
            col = "min_deposit_usd" if wait == "param:min_dep" else "platinum_threshold_usd"
            async with SessionLocal() as s:
                await s.execute(Tenant.__table__.update().where(Tenant.id == tenant_id).values(**{col: val}))
                await s.commit()
            ADMIN_WAIT.pop(key, None)
            await m.answer("Сохранено ✅")
            fake_cb = CallbackQuery(id="0", from_user=m.from_user, message=m, data="adm:params")
            await adm_params(fake_cb)  # type: ignore[arg-type]
            return

        # PB secret
        if wait == "/set_pb_secret":
            val = m.text.strip()
            if val == "-":
                val = None
            async with SessionLocal() as s:
                await s.execute(Tenant.__table__.update().where(Tenant.id == tenant_id).values(pb_secret=val))
                await s.commit()
            ADMIN_WAIT.pop(key, None)
            await m.answer("PB Secret сохранён.")
            fake_cb = CallbackQuery(id="0", from_user=m.from_user, message=m, data="adm:links")
            await adm_links(fake_cb)  # type: ignore[arg-type]

    # ---- Content editor
    def kb_content_langs() -> InlineKeyboardMarkup:
        rows = [
            [InlineKeyboardButton(text="Русский", callback_data="adm:content:lang:ru"),
             InlineKeyboardButton(text="English", callback_data="adm:content:lang:en")],
            [InlineKeyboardButton(text="हिन्दी", callback_data="adm:content:lang:hi"),
             InlineKeyboardButton(text="Español", callback_data="adm:content:lang:es")],
            [InlineKeyboardButton(text="↩️ В меню", callback_data="adm:menu")],
        ]
        return InlineKeyboardMarkup(inline_keyboard=rows)

    def kb_content_screens(lang: str) -> InlineKeyboardMarkup:
        screens = [
            ("menu", "Главное меню"),
            ("howto", "Инструкция"),
            ("subscribe", "Подписка"),
            ("register", "Регистрация"),
            ("deposit", "Депозит"),
            ("unlocked", "Доступ открыт"),
            ("platinum", "Platinum"),
            ("admin", "Экран админки"),
        ]
        rows = [[InlineKeyboardButton(text=title, callback_data=f"adm:content:edit:{lang}:{code}")]
                for code, title in screens]
        rows.append([InlineKeyboardButton(text="↩️ Языки", callback_data="adm:content")])
        rows.append([InlineKeyboardButton(text="↩️ В меню", callback_data="adm:menu")])
        return InlineKeyboardMarkup(inline_keyboard=rows)

    def kb_content_editor(lang: str, screen: str, snapshot: dict) -> InlineKeyboardMarkup:
        rows = [
            [InlineKeyboardButton(text="🖼 Изменить картинку", callback_data=f"adm:content:img:{lang}:{screen}")],
            [InlineKeyboardButton(text="✏️ Изменить заголовок", callback_data=f"adm:content:title:{lang}:{screen}")],
            [InlineKeyboardButton(text="⌨️ Изменить текст кнопки", callback_data=f"adm:content:btn:{lang}:{screen}")],
            [InlineKeyboardButton(text="♻️ Сбросить к дефолту", callback_data=f"adm:content:reset:{lang}:{screen}")],
            [InlineKeyboardButton(text="📋 Список экранов", callback_data=f"adm:content:list:{lang}")],
            [InlineKeyboardButton(text="🌐 Языки", callback_data="adm:content")],
            [InlineKeyboardButton(text="↩️ В меню", callback_data="adm:menu")],
        ]
        return InlineKeyboardMarkup(inline_keyboard=rows)

    @router.callback_query(F.data == "adm:content")
    async def adm_content(c: CallbackQuery):
        if not await is_owner(tenant_id, c.from_user.id): return
        await send_screen(c.bot, tenant_id, c.message.chat.id, "ru", "admin", "🧩 Редактор контента — выберите язык", kb_content_langs())
        await c.answer()

    @router.callback_query(F.data.startswith("adm:content:lang:"))
    async def adm_content_lang(c: CallbackQuery):
        if not await is_owner(tenant_id, c.from_user.id): return
        lang = c.data.split(":")[-1]
        await send_screen(c.bot, tenant_id, c.message.chat.id, "ru", "admin", f"Язык: {lang.upper()}\nВыберите экран:", kb_content_screens(lang))
        await c.answer()

    @router.callback_query(F.data.startswith("adm:content:list:"))
    async def adm_content_list(c: CallbackQuery):
        if not await is_owner(tenant_id, c.from_user.id): return
        lang = c.data.split(":")[-1]
        await adm_content_lang(CallbackQuery(id="0", from_user=c.from_user, message=c.message, data=f"adm:content:lang:{lang}"))  # type: ignore[arg-type]
        await c.answer()

    @router.callback_query(F.data.startswith("adm:content:edit:"))
    async def adm_content_edit(c: CallbackQuery):
        if not await is_owner(tenant_id, c.from_user.id): return
        _, _, _, lang, screen = c.data.split(":")
        title = await resolve_title(tenant_id, lang, screen)
        btn_tx = await resolve_primary_btn_text(tenant_id, lang, screen) or "—"
        img = await resolve_image(tenant_id, lang, screen)
        text = f"🧩 Редактор — {screen} ({lang.upper()})\n\nЗаголовок: <b>{title}</b>\nТекст кнопки: <code>{btn_tx}</code>\nКартинка: {'дефолт' if not img else 'кастом'}"
        await send_screen(c.bot, tenant_id, c.message.chat.id, "ru", "admin", text, kb_content_editor(lang, screen, {}))
        await c.answer()

    @router.callback_query(F.data.startswith("adm:content:title:"))
    async def adm_content_title(c: CallbackQuery):
        if not await is_owner(tenant_id, c.from_user.id): return
        _, _, _, lang, screen = c.data.split(":")
        ADMIN_WAIT[(tenant_id, c.from_user.id)] = f"content_title:{lang}:{screen}"
        await c.message.answer("Пришлите новый заголовок одним сообщением.")
        await c.answer()

    @router.callback_query(F.data.startswith("adm:content:btn:"))
    async def adm_content_btn(c: CallbackQuery):
        if not await is_owner(tenant_id, c.from_user.id): return
        _, _, _, lang, screen = c.data.split(":")
        ADMIN_WAIT[(tenant_id, c.from_user.id)] = f"content_btn:{lang}:{screen}"
        await c.message.answer("Пришлите новый текст основной кнопки одним сообщением.")
        await c.answer()

    @router.callback_query(F.data.startswith("adm:content:img:"))
    async def adm_content_img(c: CallbackQuery):
        if not await is_owner(tenant_id, c.from_user.id): return
        _, _, _, lang, screen = c.data.split(":")
        ADMIN_WAIT[(tenant_id, c.from_user.id)] = f"content_img:{lang}:{screen}"
        await c.message.answer("Пришлите картинку изображением (jpg/png) — можно просто переслать фото.")
        await c.answer()

    @router.callback_query(F.data.startswith("adm:content:reset:"))
    async def adm_content_reset(c: CallbackQuery):
        if not await is_owner(tenant_id, c.from_user.id): return
        _, _, _, lang, screen = c.data.split(":")
        await upsert_override(tenant_id, lang, screen, reset=True)
        await c.answer("Сброшено")
        await adm_content_edit(c)

    @router.message(F.photo)
    async def adm_content_catch_image(m: Message):
        key = (tenant_id, m.from_user.id)
        wait = ADMIN_WAIT.get(key)
        if not wait or not wait.startswith("content_img:"):
            # возможно это фото для рассылки
            state = BROADCAST_STATE.setdefault(key, {"segment": "all"})
            state["photo_id"] = m.photo[-1].file_id
            await m.answer("Фото прикреплено.")
            fake_cb = CallbackQuery(id="0", from_user=m.from_user, message=m, data="adm:bc")
            await adm_bc(fake_cb)  # type: ignore[arg-type]
            return
        _, lang, screen = wait.split(":")
        file_id = m.photo[-1].file_id
        await upsert_override(tenant_id, lang, screen, image_path=file_id)
        ADMIN_WAIT.pop(key, None)
        await m.answer("Картинка сохранена ✅")
        fake_cb = CallbackQuery(id="0", from_user=m.from_user, message=m, data=f"adm:content:edit:{lang}:{screen}")
        await adm_content_edit(fake_cb)  # type: ignore[arg-type]

    # ---- Params
    def kb_params(tnt: Tenant) -> InlineKeyboardMarkup:
        mark_sub = "✅" if (tnt.check_subscription or tnt.check_subscription is None) else "❌"
        mark_dep = "✅" if (tnt.check_deposit or tnt.check_deposit is None) else "❌"
        rows = [
            [InlineKeyboardButton(text="🔒 Регистрация", callback_data="adm:param:reg_locked")],
            [
                InlineKeyboardButton(text=f"{mark_sub} Проверка подписки", callback_data="adm:param:toggle:sub"),
                InlineKeyboardButton(text=f"💵 Мин. деп: {int(tnt.min_deposit_usd or 0)}$", callback_data="adm:param:set:min_dep"),
            ],
            [
                InlineKeyboardButton(text=f"{mark_dep} Проверка депозита", callback_data="adm:param:toggle:dep"),
                InlineKeyboardButton(text=f"💠 Порог Platinum: {int(tnt.platinum_threshold_usd or 0)}$", callback_data="adm:param:set:plat"),
            ],
            [InlineKeyboardButton(text="↩️ В меню", callback_data="adm:menu")],
        ]
        return InlineKeyboardMarkup(inline_keyboard=rows)

    @router.callback_query(F.data == "adm:params")
    async def adm_params(c: CallbackQuery):
        if not await is_owner(tenant_id, c.from_user.id): return
        tnt = await get_tenant(tenant_id)
        await send_screen(c.bot, tenant_id, c.message.chat.id, "ru", "admin", "⚙️ Параметры", kb_params(tnt))
        await c.answer()

    @router.callback_query(F.data == "adm:param:reg_locked")
    async def adm_param_reg_locked(c: CallbackQuery):
        if not await is_owner(tenant_id, c.from_user.id): return
        await c.answer("Регистрацию отключать нельзя.", show_alert=True)

    @router.callback_query(F.data == "adm:param:toggle:sub")
    async def adm_param_toggle_sub(c: CallbackQuery):
        if not await is_owner(tenant_id, c.from_user.id): return
        async with SessionLocal() as s:
            res = await s.execute(select(Tenant).where(Tenant.id == tenant_id))
            tnt = res.scalar_one()
            newv = not bool(tnt.check_subscription)
            await s.execute(Tenant.__table__.update().where(Tenant.id == tenant_id).values(check_subscription=newv))
            await s.commit()
        await adm_params(c)

    @router.callback_query(F.data == "adm:param:toggle:dep")
    async def adm_param_toggle_dep(c: CallbackQuery):
        if not await is_owner(tenant_id, c.from_user.id): return
        async with SessionLocal() as s:
            res = await s.execute(select(Tenant).where(Tenant.id == tenant_id))
            tnt = res.scalar_one()
            newv = not bool(tnt.check_deposit)
            await s.execute(Tenant.__table__.update().where(Tenant.id == tenant_id).values(check_deposit=newv))
            await s.commit()
        await adm_params(c)

    @router.callback_query(F.data == "adm:param:set:min_dep")
    async def adm_param_set_min_dep(c: CallbackQuery):
        if not await is_owner(tenant_id, c.from_user.id): return
        ADMIN_WAIT[(tenant_id, c.from_user.id)] = "param:min_dep"
        await c.message.answer("Пришлите новое значение <b>минимального депозита</b> в $ (целое или дробное).", parse_mode="HTML")
        await c.answer()

    @router.callback_query(F.data == "adm:param:set:plat")
    async def adm_param_set_plat(c: CallbackQuery):
        if not await is_owner(tenant_id, c.from_user.id): return
        ADMIN_WAIT[(tenant_id, c.from_user.id)] = "param:plat"
        await c.message.answer("Пришлите новый <b>порог Platinum</b> в $ (целое или дробное).", parse_mode="HTML")
        await c.answer()

    # ---- Broadcast
    @router.callback_query(F.data == "adm:bc")
    async def adm_bc(c: CallbackQuery):
        if not await is_owner(tenant_id, c.from_user.id): return
        state = BROADCAST_STATE.setdefault((tenant_id, c.from_user.id), {"segment": "all"})
        txt = "📰 Рассылка\n\nСегмент: " + {
            "all": "всем", "reg": "с регистрацией", "dep": "с депозитом", "nosteps": "/start без шагов"
        }[state["segment"]]
        if state.get("text"):
            txt += f"\n\nТекст: {state['text'][:80]}..."
        if state.get("photo_id"):
            txt += "\n\nФото: прикреплено"
        await send_screen(c.bot, tenant_id, c.message.chat.id, "ru", "admin", txt, kb_bc_menu(state))
        await c.answer()

    @router.callback_query(F.data.startswith("adm:bc:seg:"))
    async def adm_bc_seg(c: CallbackQuery):
        if not await is_owner(tenant_id, c.from_user.id): return
        seg = c.data.split(":")[-1]
        state = BROADCAST_STATE.setdefault((tenant_id, c.from_user.id), {"segment": "all"})
        state["segment"] = seg
        await adm_bc(c)

    @router.callback_query(F.data == "adm:bc:text")
    async def adm_bc_text(c: CallbackQuery):
        if not await is_owner(tenant_id, c.from_user.id): return
        ADMIN_WAIT[(tenant_id, c.from_user.id)] = "bc_text"
        await c.message.answer("Пришлите текст рассылки одним сообщением.")
        await c.answer()

    @router.callback_query(F.data == "adm:bc:photo")
    async def adm_bc_photo(c: CallbackQuery):
        if not await is_owner(tenant_id, c.from_user.id): return
        await c.message.answer("Пришлите фотографию одним сообщением.")
        await c.answer()

    @router.callback_query(F.data == "adm:bc:run")
    async def adm_bc_run(c: CallbackQuery):
        if not await is_owner(tenant_id, c.from_user.id): return
        state = BROADCAST_STATE.get((tenant_id, c.from_user.id)) or {}
        text = state.get("text") or " "
        photo_id = state.get("photo_id")
        seg = state.get("segment", "all")

        # выбираем аудиторию
        async with SessionLocal() as s:
            q = select(UserAccess.user_id).where(UserAccess.tenant_id == tenant_id)
            if seg == "reg":
                q = q.where(UserAccess.is_registered == True)
            elif seg == "dep":
                q = q.where(UserAccess.has_deposit == True)
            elif seg == "nosteps":
                q = q.where((UserAccess.is_registered == False) & (UserAccess.has_deposit == False))
            ids = [r[0] for r in (await s.execute(q)).all()]

        bot = c.bot
        ok = 0
        for uid in ids:
            try:
                if photo_id:
                    await bot.send_photo(uid, photo=photo_id, caption=text)
                else:
                    await bot.send_message(uid, text)
                ok += 1
                await asyncio.sleep(0.05)
            except Exception:
                pass

        await c.answer(f"Отправлено: {ok}", show_alert=True)
        await adm_bc(c)

    # ---- Stats
    @router.callback_query(F.data == "adm:stats")
    async def adm_stats(c: CallbackQuery):
        if not await is_owner(tenant_id, c.from_user.id): return
        async with SessionLocal() as s:
            total = (await s.execute(select(func.count()).select_from(UserAccess).where(UserAccess.tenant_id == tenant_id))).scalar() or 0
            regs = (await s.execute(select(func.count()).select_from(UserAccess).where(UserAccess.tenant_id == tenant_id, UserAccess.is_registered == True))).scalar() or 0
            deps = (await s.execute(select(func.count()).select_from(UserAccess).where(UserAccess.tenant_id == tenant_id, UserAccess.has_deposit == True))).scalar() or 0
            plats = (await s.execute(select(func.count()).select_from(UserAccess).where(UserAccess.tenant_id == tenant_id, UserAccess.is_platinum == True))).scalar() or 0
        txt = f"📊 Статистика\n\nВсего: {total}\nРегистраций: {regs}\nДепозитов: {deps}\nPlatinum: {plats}"
        await send_screen(c.bot, tenant_id, c.message.chat.id, "ru", "admin", txt, kb_admin_main())
        await c.answer()

    return router


# =========================
#          Runner
# =========================
async def run_child_bot(token: str, tenant_id: int):
    bot = Bot(token, default=DefaultBotProperties(parse_mode="HTML"))
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(make_child_router(tenant_id))
    await dp.start_polling(bot)
