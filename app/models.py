# app/models.py
from datetime import datetime
from sqlalchemy import (
    BigInteger,
    String,
    Boolean,
    DateTime,
    Integer,
    UniqueConstraint,
    Text,
    Float,
)
from sqlalchemy.orm import Mapped, mapped_column
from app.db import Base
from sqlalchemy.types import JSON as SA_JSON
from sqlalchemy.ext.mutable import MutableDict

class Tenant(Base):
    __tablename__ = "tenants"
    __table_args__ = (UniqueConstraint("owner_telegram_id", name="uq_tenant_owner"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    owner_telegram_id: Mapped[int] = mapped_column(BigInteger, index=True, unique=True)

    bot_token: Mapped[str | None] = mapped_column(String(255), unique=True)
    bot_username: Mapped[str | None] = mapped_column(String(64), index=True)

    # Канал тенанта (гейт "Подписка")
    gate_channel_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    gate_channel_url: Mapped[str | None] = mapped_column(String(255), nullable=True)

    # Уникальные ссылки этого тенанта
    ref_link: Mapped[str | None] = mapped_column(String(512), nullable=True)
    deposit_link: Mapped[str | None] = mapped_column(String(512), nullable=True)

    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    pb_secret = mapped_column(String(64))
    support_url = mapped_column(String(255))

    # --- Параметры гейта ---
    check_subscription = mapped_column(Boolean, default=True)  # Проверять подписку
    check_deposit = mapped_column(Boolean, default=True)  # Проверять депозит
    min_deposit_usd = mapped_column(Float, default=10.0)  # Минимальный суммарный деп
    platinum_threshold_usd = mapped_column(Float, default=500.0)

class UserLang(Base):
    __tablename__ = "user_lang"
    __table_args__ = (UniqueConstraint("tenant_id", "user_id", name="uq_user_lang"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tenant_id: Mapped[int] = mapped_column(Integer, index=True)
    user_id: Mapped[int] = mapped_column(BigInteger, index=True)
    lang: Mapped[str] = mapped_column(String(5), default="ru")


class UserState(Base):
    __tablename__ = "user_state"
    __table_args__ = (UniqueConstraint("tenant_id", "chat_id", name="uq_user_state_chat"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tenant_id: Mapped[int] = mapped_column(Integer, index=True)
    chat_id: Mapped[int] = mapped_column(BigInteger, index=True)
    last_bot_message_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )


class UserAccess(Base):
    __tablename__ = "user_access"
    __table_args__ = (UniqueConstraint("tenant_id", "user_id", name="uq_user_access"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tenant_id: Mapped[int] = mapped_column(Integer, index=True)
    user_id: Mapped[int] = mapped_column(BigInteger, index=True)

    is_registered: Mapped[bool] = mapped_column(Boolean, default=False)
    has_deposit: Mapped[bool] = mapped_column(Boolean, default=False)
    unlocked_shown: Mapped[bool] = mapped_column(Boolean, default=False)

    click_id: Mapped[str | None] = mapped_column(String(64), unique=True, index=True)
    trader_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    total_deposits: Mapped[int] = mapped_column(Integer, default=0)
    username: Mapped[str | None] = mapped_column(String(64), nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )
    is_platinum = mapped_column(Boolean, default=False)
    platinum_shown = mapped_column(Boolean, default=False)

class Event(Base):
    """Сырые события от постбэков для аналитики по каждому тенанту."""
    __tablename__ = "events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tenant_id: Mapped[int] = mapped_column(Integer, index=True)
    user_id: Mapped[int | None] = mapped_column(BigInteger, index=True, nullable=True)
    click_id: Mapped[str] = mapped_column(String(64), index=True)
    kind: Mapped[str] = mapped_column(String(16))            # "reg" | "ftd" | "rd"
    amount: Mapped[float | None] = mapped_column(Float, nullable=True)
    raw_qs: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

# ... твои существующие импорты и модели выше ...

class ContentOverride(Base):
    """
    Кастомизация контента по тенанту/языку/экрану.
    screen: 'menu','howto','subscribe','register','deposit','unlocked','platinum','lang'
    """
    __tablename__ = "content_override"
    __table_args__ = (UniqueConstraint("tenant_id", "lang", "screen", name="uq_content_override"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tenant_id: Mapped[int] = mapped_column(Integer, index=True)
    lang: Mapped[str] = mapped_column(String(5), index=True)
    screen: Mapped[str] = mapped_column(String(32), index=True)

    title: Mapped[str | None] = mapped_column(String(512), nullable=True)             # заголовок экрана
    primary_btn_text: Mapped[str | None] = mapped_column(String(128), nullable=True)  # главный CTA текста кнопки
    photo_file_id: Mapped[str | None] = mapped_column(String(256), nullable=True)     # file_id фото из Telegram

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    body_html: Mapped[str | None] = mapped_column(Text, nullable=True)
    buttons_json: Mapped[dict | None] = mapped_column(
        MutableDict.as_mutable(SA_JSON), nullable=True
    )