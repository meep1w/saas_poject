from __future__ import annotations

import os
from typing import List, Optional
from urllib.parse import urlencode

from pydantic import BaseModel
from dotenv import load_dotenv, find_dotenv

# Подхватываем .env из корня проекта
load_dotenv(find_dotenv())


def _parse_admin_ids(raw: str) -> List[int]:
    if not raw:
        return []
    out: List[int] = []
    for part in raw.replace(";", ",").split(","):
        part = part.strip()
        if not part:
            continue
        # допускаем записи вида "12345  # коммент"
        part = part.split()[0]
        if part.lstrip("+-").isdigit():
            try:
                out.append(int(part))
            except ValueError:
                pass
    return out


def _normalize_base(url: str) -> str:
    # убираем хвостовой /, чтобы было "https://example.com" (а не со слэшем)
    return url[:-1] if url.endswith("/") else url


class Settings(BaseModel):
    # Боты / каналы
    PARENT_BOT_TOKEN: str = os.getenv("PARENT_BOT_TOKEN", "")
    PRIVATE_CHANNEL_ID: int = int(os.getenv("PRIVATE_CHANNEL_ID", "0"))

    # Ссылки для шагов
    CHANNEL_URL: str = os.getenv("CHANNEL_URL", "https://t.me/your_channel")
    REF_LINK: str = os.getenv("REF_LINK", "https://example.com/ref")
    DEPOSIT_LINK: str = os.getenv("DEPOSIT_LINK", "https://example.com/deposit")
    MINIAPP_URL: str = os.getenv("MINIAPP_URL", "https://example.com/miniapp")
    PLATINUM_MINIAPP_URL: str = os.getenv(
        "PLATINUM_MINIAPP_URL", "https://meep1w.github.io/pocketai-vip-miniapp/"
    )

    # База
    DATABASE_URL: str = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///./saas.db")

    # Постбэки
    POSTBACK_BASE: str = _normalize_base(os.getenv("POSTBACK_BASE", "https://YOUR-DOMAIN"))

    # Админы
    GA_ADMIN_IDS: List[int] = _parse_admin_ids(os.getenv("GA_ADMIN_IDS", ""))

    # Прочее
    SUPPORT_URL: str = os.getenv("SUPPORT_URL", "https://t.me/support")
    LANG_DEFAULT: str = os.getenv("LANG_DEFAULT", "ru")
    CLICK_SALT: str = os.getenv("CLICK_SALT", "dev_salt_change_me")

    # -------------------------
    # Удобные хелперы для ПП
    # -------------------------
    def make_pp_url(
        self,
        path: str,  # "/pp/reg" | "/pp/ftd" | "/pp/rd"
        *,
        click_id: str,
        tid: int,
        secret: str,
        trader_id: Optional[str] = None,
        sumdep: Optional[str] = None,
        extra: Optional[dict] = None,
    ) -> str:
        """Собирает корректный URL постбэка с нужными параметрами."""
        if not path.startswith("/"):
            path = "/" + path
        qs = {
            "click_id": click_id,
            "tid": tid,
            "secret": secret,
        }
        if trader_id is not None:
            qs["trader_id"] = trader_id
        if sumdep is not None:
            qs["sumdep"] = str(sumdep)
        if extra:
            # не перезатираем основные ключи
            for k, v in extra.items():
                if k not in qs and v is not None:
                    qs[k] = v
        return f"{self.POSTBACK_BASE}{path}?{urlencode(qs)}"

    def pp_reg_url(self, *, click_id: str, tid: int, secret: str, trader_id: Optional[str] = None) -> str:
        return self.make_pp_url("/pp/reg", click_id=click_id, tid=tid, secret=secret, trader_id=trader_id)

    def pp_ftd_url(self, *, click_id: str, tid: int, secret: str, trader_id: Optional[str] = None, sumdep: str | float | int = "0") -> str:
        return self.make_pp_url("/pp/ftd", click_id=click_id, tid=tid, secret=secret, trader_id=trader_id, sumdep=str(sumdep))

    def pp_rd_url(self, *, click_id: str, tid: int, secret: str, trader_id: Optional[str] = None, sumdep: str | float | int = "0") -> str:
        return self.make_pp_url("/pp/rd", click_id=click_id, tid=tid, secret=secret, trader_id=trader_id, sumdep=str(sumdep))


# ВАЖНО: для SQLite должен быть async-драйвер: sqlite+aiosqlite:///
# Для Postgres — postgresql+asyncpg://user:pass@host:port/db
settings = Settings()
