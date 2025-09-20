from aiogram import Bot, Dispatcher, Router, F
from aiogram.client.default import DefaultBotProperties
from aiogram.types import Message
from aiogram.filters import Command
from aiogram.exceptions import TelegramForbiddenError

from sqlalchemy import select

from app.settings import settings
from app.db import SessionLocal
from app.models import Tenant

router = Router()

WELCOME_OK_RU = (
    "Привет! Вы член моей приватки — доступ разрешён.\n"
    "Отправьте API-ТОКЕН вашего бота, которого хотите подключить.\n"
    "Важно: можно подключить только *1 бота*."
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

async def run_parent():
    bot = Bot(settings.PARENT_BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
    dp = Dispatcher()
    dp.include_router(router)
    await dp.start_polling(bot)
