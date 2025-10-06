from __future__ import annotations

import re
import asyncio
import secrets as _pysecrets
from datetime import datetime
from typing import Optional

from fastapi import FastAPI, Query
from sqlalchemy import select
from urllib.parse import urlencode

from aiogram import Bot
from aiogram.client.default import DefaultBotProperties

from app.db import SessionLocal
from app.models import UserAccess, Event, Tenant
from app.settings import settings
from app.bots.child.bot_instance import (
    t, add_params, get_lang, mark_unlocked_shown, mark_platinum_shown,
    send_screen, kb_register, kb_deposit, kb_open_app, kb_open_platinum,
    user_deposit_sum,
)

app = FastAPI(title="Local Postbacks")


# =========================
#      Small helpers
# =========================
def _ok(**extra):
    d = {"status": "ok"}
    d.update(extra)
    return d


def _err(msg, **extra):
    d = {"status": "error", "message": msg}
    d.update(extra)
    return d


def _nf(**extra):
    d = {"status": "not_found"}
    d.update(extra)
    return d


def _parse_amount(raw) -> Optional[float]:
    """
    Надёжный парсер сумм из партнёрки.
    Принимает '100', '100.00', '100,00', 'USD 100.50', '100 000,25' и т.п.
    Возвращает float или None.
    """
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None
    s = s.replace(",", ".")                    # запятая → точка
    s = re.sub(r"[^0-9.\-]", "", s)            # убираем всё кроме цифр/точек/минуса
    if s.count(".") > 1:
        parts = [p for p in s.split(".") if p != ""]
        if not parts:
            return None
        s = parts[0] + "." + "".join(parts[1:])
    try:
        return float(s)
    except ValueError:
        return None


# =========================
#       DB helpers
# =========================
async def _load_by_click(click_id: str) -> Optional[UserAccess]:
    async with SessionLocal() as s:
        res = await s.execute(select(UserAccess).where(UserAccess.click_id == click_id))
        return res.scalar_one_or_none()


async def _get_tenant(tid: int) -> Optional[Tenant]:
    async with SessionLocal() as s:
        r = await s.execute(select(Tenant).where(Tenant.id == tid))
        return r.scalar_one_or_none()


async def _ensure_pb_secret(tenant_id: int) -> str:
    """
    Лениво генерируем pb_secret, если пустой. Возвращаем актуальный секрет.
    """
    async with SessionLocal() as s:
        r = await s.execute(select(Tenant).where(Tenant.id == tenant_id))
        tnt = r.scalar_one()
        if not tnt.pb_secret:
            new_secret = _pysecrets.token_urlsafe(20)  # ~27 символов, URL-safe
            await s.execute(
                Tenant.__table__.update()
                .where(Tenant.id == tenant_id)
                .values(pb_secret=new_secret)
            )
            await s.commit()
            return new_secret
        return tnt.pb_secret


async def _log_event(kind: str, ua: Optional[UserAccess], params: dict):
    """
    Сохраняем сырое событие. Пишем trader_id и корректно парсим сумму.
    """
    raw = urlencode({k: "" if v is None else v for k, v in params.items()})
    amt = _parse_amount(params.get("sumdep"))
    async with SessionLocal() as s:
        await s.execute(Event.__table__.insert().values(
            tenant_id=(ua.tenant_id if ua else None),
            user_id=(ua.user_id if ua else None),
            click_id=(ua.click_id if ua else params.get("click_id")),
            trader_id=params.get("trader_id"),
            kind=kind,
            amount=amt,
            raw_qs=raw,
            created_at=datetime.utcnow(),
        ))
        await s.commit()


# Надёжная отправка экрана с ретраем и логами
async def _safe_send_screen(
    *, screen: str, bot: Bot, tenant_id: int, chat_id: int,
    click_id: str | None, lang: str, text: str, kb
):
    last_exc = None
    for _ in range(2):  # две попытки
        try:
            await send_screen(bot, tenant_id, chat_id, lang, screen, text, kb)
            try:
                await _log_event("push_sent", None, {
                    "click_id": click_id or str(chat_id),
                    "screen": screen, "ok": "1"
                })
            except Exception:
                pass
            return
        except Exception as e:
            last_exc = e
            await asyncio.sleep(0.6)
    try:
        await _log_event("push_error", None, {
            "click_id": click_id or str(chat_id),
            "screen": screen, "err": str(last_exc)
        })
    except Exception:
        pass
    raise last_exc


async def _push_next_screen(ua_id: int):
    """
    Пушим следующий “шаг” пользователю (после входящего постбэка).
    Учитываем параметры тенанта: проверка депозита, пороги и т.д.
    """
    async with SessionLocal() as s:
        res = await s.execute(select(UserAccess).where(UserAccess.id == ua_id))
        ua = res.scalar_one()
        t_res = await s.execute(select(Tenant).where(Tenant.id == ua.tenant_id))
        tenant = t_res.scalar_one()

    bot = Bot(tenant.bot_token, default=DefaultBotProperties(parse_mode="HTML"))
    chat_id = ua.user_id
    lang = await get_lang(ua.tenant_id, ua.user_id)
    support_url = tenant.support_url or settings.SUPPORT_URL

    try:
        # если ещё не регистрирован — покажем регистрацию
        if not ua.is_registered:
            ref_url = add_params(tenant.ref_link or settings.REF_LINK, click_id=ua.click_id, tid=ua.tenant_id)
            await _safe_send_screen(
                screen="register", bot=bot, tenant_id=ua.tenant_id, chat_id=chat_id,
                click_id=ua.click_id, lang=lang,
                text=f"<b>{t(lang,'gate_reg_title')}</b>\n\n{t(lang,'gate_reg_text')}",
                kb=kb_register(lang, ref_url)
            )
            return

        # если проверка депозита включена — проверим минимум (None = включено)
        if tenant.check_deposit is not False:
            total = await user_deposit_sum(ua.tenant_id, ua.click_id)
            need = float(tenant.min_deposit_usd or 0.0)
            if total < need:
                def fmt(x: float) -> str:
                    return f"{int(x)}" if abs(x - int(x)) < 1e-9 else f"{x:.2f}"

                remain = max(need - total, 0.0)
                hints = {
                    "ru": (f"\n\n<b>Минимальный депозит:</b> {fmt(need)}$"
                           f"\n<b>Внесено:</b> {fmt(total)}$"
                           f"\n<b>Осталось внести:</b> {fmt(remain)}$"),
                    "en": (f"\n\n<b>Minimum deposit:</b> {fmt(need)}$"
                           f"\n<b>Deposited:</b> {fmt(total)}$"
                           f"\n<b>Left to deposit:</b> {fmt(remain)}$"),
                    "hi": (f"\n\n<b>न्यूनतम जमा:</b> {fmt(need)}$"
                           f"\n<b>जमा किया गया:</b> {fmt(total)}$"
                           f"\n<b>बाकी जमा करना:</b> {fmt(remain)}$"),
                    "es": (f"\n\n<b>Depósito mínimo:</b> {fmt(need)}$"
                           f"\n<b>Depositado:</b> {fmt(total)}$"
                           f"\n<b>Falta depositar:</b> {fmt(remain)}$"),
                }

                dep_url = add_params(tenant.deposit_link or settings.DEPOSIT_LINK,
                                     click_id=ua.click_id, tid=ua.tenant_id)
                text = (f"<b>{t(lang, 'gate_dep_title')}</b>\n\n"
                        f"{t(lang, 'gate_dep_text')}{hints.get(lang, hints['en'])}")
                await _safe_send_screen(
                    screen="deposit", bot=bot, tenant_id=ua.tenant_id, chat_id=chat_id,
                    click_id=ua.click_id, lang=lang, text=text, kb=kb_deposit(lang, dep_url)
                )
                return

        # platinum экран
        if ua.is_platinum and not ua.platinum_shown:
            await _safe_send_screen(
                screen="platinum", bot=bot, tenant_id=ua.tenant_id, chat_id=chat_id,
                click_id=ua.click_id, lang=lang,
                text=f"<b>{t(lang,'platinum_title')}</b>\n\n{t(lang,'platinum_text')}",
                kb=kb_open_platinum(lang, support_url)
            )
            await mark_platinum_shown(ua.tenant_id, ua.user_id)
            return

        # доступ открыт
        if not ua.unlocked_shown:
            await _safe_send_screen(
                screen="unlocked", bot=bot, tenant_id=ua.tenant_id, chat_id=chat_id,
                click_id=ua.click_id, lang=lang,
                text=f"<b>{t(lang,'unlocked_title')}</b>\n\n{t(lang,'unlocked_text')}",
                kb=kb_open_app(lang, support_url)
            )
            await mark_unlocked_shown(ua.tenant_id, ua.user_id)
            return

    except Exception as e:
        # На всякий случай продублируем лог ошибки
        try:
            await _log_event("push_error", ua, {"click_id": ua.click_id, "err": str(e)})
        except Exception:
            pass
        raise
    finally:
        await bot.session.close()


async def _check_secret(tenant_id: int, secret: Optional[str]) -> bool:
    """
    Секрет обязателен у всех: если пуст — генерим, затем сравниваем строго.
    """
    tnt = await _get_tenant(tenant_id)
    if not tnt:
        return False
    must = tnt.pb_secret or await _ensure_pb_secret(tenant_id)
    return secret == must


# =========================
#         Endpoints
# =========================
@app.get("/pp/reg")
async def pp_reg(
    click_id: str = Query(...),
    trader_id: Optional[str] = None,
    tid: Optional[int] = None,
    secret: Optional[str] = None,
):
    ua = await _load_by_click(click_id)
    if not ua:
        return _nf(click_id=click_id)
    tenant_id = tid or ua.tenant_id
    if not await _check_secret(tenant_id, secret):
        await _log_event("reg", ua, {"click_id": click_id, "trader_id": trader_id, "tid": tenant_id, "secret": secret})
        return _err("bad_secret")

    async with SessionLocal() as s:
        vals = {"is_registered": True}
        if trader_id and not ua.trader_id:
            vals["trader_id"] = trader_id
        await s.execute(UserAccess.__table__.update().where(UserAccess.id == ua.id).values(**vals))
        await s.commit()

    await _log_event("reg", ua, {"click_id": click_id, "trader_id": trader_id, "tid": tenant_id})
    await _push_next_screen(ua.id)
    return _ok()


@app.get("/pp/ftd")
async def pp_ftd(
    click_id: str = Query(...),
    sumdep: Optional[str] = None,                      # строкой — сами парсим
    sum_alt: Optional[str] = Query(None, alias="sum"), # алиас, если партнёрка шлёт ?sum=
    amount_alt: Optional[str] = Query(None, alias="amount"),  # алиас ?amount=
    trader_id: Optional[str] = None,
    tid: Optional[int] = None,
    secret: Optional[str] = None,
):
    ua = await _load_by_click(click_id)
    if not ua:
        return _nf(click_id=click_id)
    tenant_id = tid or ua.tenant_id
    if not await _check_secret(tenant_id, secret):
        eff_sum = sumdep or sum_alt or amount_alt
        await _log_event("ftd", ua, {"click_id": click_id, "sumdep": eff_sum, "tid": tenant_id, "secret": secret})
        return _err("bad_secret")

    eff_sum = sumdep or sum_alt or amount_alt

    # ставим флаг "есть депозит" на FTD сразу
    async with SessionLocal() as s:
        vals = {
            "has_deposit": True,
            "total_deposits": max(1, (ua.total_deposits or 0)),
        }
        if trader_id and not ua.trader_id:
            vals["trader_id"] = trader_id
        await s.execute(UserAccess.__table__.update().where(UserAccess.id == ua.id).values(**vals))
        await s.commit()

    # лог с пробросом trader_id и нормализуемой суммой
    await _log_event("ftd", ua, {"click_id": click_id, "sumdep": eff_sum, "tid": tenant_id, "trader_id": trader_id})

    # platinum check
    tnt = await _get_tenant(tenant_id)
    thr = float((tnt.platinum_threshold_usd or 500.0))
    total = await user_deposit_sum(tenant_id, ua.click_id)
    if (not ua.is_platinum) and total >= thr:
        async with SessionLocal() as s:
            await s.execute(
                UserAccess.__table__
                .update()
                .where(UserAccess.id == ua.id)
                .values(is_platinum=True, platinum_shown=False)  # сбрасываем, чтобы экран показался
            )
            await s.commit()

    await _push_next_screen(ua.id)
    return _ok(first_time=True, amount=_parse_amount(eff_sum))


@app.get("/pp/rd")
async def pp_rd(
    click_id: str = Query(...),
    sumdep: Optional[str] = None,                      # строкой — сами парсим
    sum_alt: Optional[str] = Query(None, alias="sum"),
    amount_alt: Optional[str] = Query(None, alias="amount"),
    trader_id: Optional[str] = None,
    tid: Optional[int] = None,
    secret: Optional[str] = None,
):
    ua = await _load_by_click(click_id)
    if not ua:
        return _nf(click_id=click_id)
    tenant_id = tid or ua.tenant_id
    if not await _check_secret(tenant_id, secret):
        eff_sum = sumdep or sum_alt or amount_alt
        await _log_event("rd", ua, {"click_id": click_id, "sumdep": eff_sum, "tid": tenant_id, "secret": secret})
        return _err("bad_secret")

    eff_sum = sumdep or sum_alt or amount_alt

    new_count = (ua.total_deposits or 0) + 1
    async with SessionLocal() as s:
        vals = {
            "has_deposit": True,              # RD тоже помечаем как "депозит есть"
            "total_deposits": new_count,
        }
        if trader_id and not ua.trader_id:
            vals["trader_id"] = trader_id
        await s.execute(UserAccess.__table__.update().where(UserAccess.id == ua.id).values(**vals))
        await s.commit()

    # лог с пробросом trader_id
    await _log_event("rd", ua, {"click_id": click_id, "sumdep": eff_sum, "tid": tenant_id, "trader_id": trader_id})

    # platinum check
    tnt = await _get_tenant(tenant_id)
    thr = float((tnt.platinum_threshold_usd or 500.0))
    total = await user_deposit_sum(tenant_id, ua.click_id)
    if (not ua.is_platinum) and total >= thr:
        async with SessionLocal() as s:
            await s.execute(
                UserAccess.__table__
                .update()
                .where(UserAccess.id == ua.id)
                .values(is_platinum=True, platinum_shown=False)
            )
            await s.commit()

    await _push_next_screen(ua.id)
    return _ok(total_deposits=new_count, amount=_parse_amount(eff_sum))


@app.get("/pp/debug")
async def pp_debug(click_id: str = Query(...)):
    ua = await _load_by_click(click_id)
    if not ua:
        return _nf(click_id=click_id)
    total = await user_deposit_sum(ua.tenant_id, ua.click_id)
    return _ok(
        tenant_id=ua.tenant_id, user_id=ua.user_id,
        is_registered=ua.is_registered, has_deposit=ua.has_deposit,
        trader_id=ua.trader_id, total_deposits=ua.total_deposits,
        is_platinum=ua.is_platinum, platinum_shown=ua.platinum_shown,
        total_amount=total,
        unlocked_shown=ua.unlocked_shown,
    )
