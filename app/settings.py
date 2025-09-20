import os
from pydantic import BaseModel
from dotenv import load_dotenv, find_dotenv

# Подхватываем .env из корня проекта
load_dotenv(find_dotenv())

class Settings(BaseModel):
    PARENT_BOT_TOKEN: str = os.getenv("PARENT_BOT_TOKEN", "")
    PRIVATE_CHANNEL_ID: int = int(os.getenv("PRIVATE_CHANNEL_ID", "0"))  # для проверки членства

    # Ссылки для шагов
    CHANNEL_URL: str = os.getenv("CHANNEL_URL", "https://t.me/your_channel")
    REF_LINK: str = os.getenv("REF_LINK", "https://example.com/ref")
    DEPOSIT_LINK: str = os.getenv("DEPOSIT_LINK", "https://example.com/deposit")
    MINIAPP_URL: str = os.getenv("MINIAPP_URL", "https://example.com/miniapp")
    POSTBACK_BASE: str = os.getenv("POSTBACK_BASE", "https://YOUR-DOMAIN")
    # БД/прочее
    DATABASE_URL: str = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///./saas.db")
    GA_ADMIN_IDS: list[int] = (
        [int(x) for x in os.getenv("GA_ADMIN_IDS", "").split(",") if x.strip().isdigit()]
    )
    SUPPORT_URL: str = os.getenv("SUPPORT_URL", "https://t.me/support")
    LANG_DEFAULT: str = os.getenv("LANG_DEFAULT", "ru")

    CLICK_SALT: str = os.getenv("CLICK_SALT", "dev_salt_change_me")
    PLATINUM_MINIAPP_URL: str = os.getenv(
        "PLATINUM_MINIAPP_URL", "https://meep1w.github.io/pocketai-vip-miniapp/"
    )


settings = Settings()
