from __future__ import annotations

from datetime import datetime
from typing import Optional

from fastapi import FastAPI, Query
from sqlalchemy import select, func
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


async def _load_by_click(click_id: str) -> Optional[UserAccess]:
    async with SessionLocal() as s:
        res = await s.execute(select(UserAccess).where(UserAccess.click_id == click_id))
        return res.scalar_one_or_none()


async def _get_tenant(tid: int) -> Optional[Tenant]:
    async with SessionLocal() as s:
        r = await s.execute(select(Tenant).where(Tenant.id == tid))
        return r.scalar_one_or_none()


async def _log_event(kind: str, ua: Optional[UserAccess], params: dict):
    raw = urlencode({k: "" if v is None else v for k, v in params.items()})
    async with SessionLocal() as s:
        await s.execute(Event.__table__.insert().values(
            tenant_id=(ua.tenant_id if ua else None),
            user_id=(ua.user_id if ua else None),
            click_id=(ua.click_id if ua else params.get("click_id")),
            kind=kind,
            amount=float(params.get("sumdep")) if params.get("sumdep") not in (None, "") else None,
            raw_qs=raw,
            created_at=datetime.utcnow(),
        ))
        await s.commit()


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
            await send_screen(bot, ua.tenant_id, chat_id, lang, "register",
                              f"<b>{t(lang,'gate_reg_title')}</b>\n\n{t(lang,'gate_reg_text')}",
                              kb_register(lang, ref_url))
            return

        # если проверка депозита включена — проверим минимум
        # стало
        if tenant.check_deposit:
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
                await send_screen(bot, ua.tenant_id, chat_id, lang, "deposit",
                                  text, kb_deposit(lang, dep_url))
                return

        # platinum
        if ua.is_platinum and not ua.platinum_shown:
            await send_screen(bot, ua.tenant_id, chat_id, lang, "platinum",
                              f"<b>{t(lang,'platinum_title')}</b>\n\n{t(lang,'platinum_text')}",
                              kb_open_platinum(lang, support_url))
            await mark_platinum_shown(ua.tenant_id, ua.user_id)
            return

        # доступ открыт
        if not ua.unlocked_shown:
            await send_screen(bot, ua.tenant_id, chat_id, lang, "unlocked",
                              f"<b>{t(lang,'unlocked_title')}</b>\n\n{t(lang,'unlocked_text')}",
                              kb_open_app(lang, support_url))
            await mark_unlocked_shown(ua.tenant_id, ua.user_id)
            return
    finally:
        await bot.session.close()


async def _check_secret(tenant_id: int, secret: Optional[str]) -> bool:
    tnt = await _get_tenant(tenant_id)
    if not tnt:
        return False
    if not tnt.pb_secret:  # секрет отключён — пропускаем проверку
        return True
    return secret == tnt.pb_secret


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
    sumdep: Optional[float] = None,
    trader_id: Optional[str] = None,
    tid: Optional[int] = None,
    secret: Optional[str] = None,
):
    ua = await _load_by_click(click_id)
    if not ua:
        return _nf(click_id=click_id)
    tenant_id = tid or ua.tenant_id
    if not await _check_secret(tenant_id, secret):
        await _log_event("ftd", ua, {"click_id": click_id, "sumdep": sumdep, "tid": tenant_id, "secret": secret})
        return _err("bad_secret")

    # ставим флаг "есть депозит" на FTD сразу
    async with SessionLocal() as s:
        vals = {"has_deposit": True, "total_deposits": max(1, (ua.total_deposits or 0))}
        if trader_id and not ua.trader_id:
            vals["trader_id"] = trader_id
        await s.execute(UserAccess.__table__.update().where(UserAccess.id == ua.id).values(**vals))
        await s.commit()

    await _log_event("ftd", ua, {"click_id": click_id, "sumdep": sumdep, "tid": tenant_id})

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
                .values(is_platinum=True, platinum_shown=False)  # <= сбрасываем, чтобы экран показался
            )
            await s.commit()

    await _push_next_screen(ua.id)
    return _ok(first_time=True, amount=sumdep)


@app.get("/pp/rd")
async def pp_rd(
    click_id: str = Query(...),
    sumdep: Optional[float] = None,
    tid: Optional[int] = None,
    secret: Optional[str] = None,
):
    ua = await _load_by_click(click_id)
    if not ua:
        return _nf(click_id=click_id)
    tenant_id = tid or ua.tenant_id
    if not await _check_secret(tenant_id, secret):
        await _log_event("rd", ua, {"click_id": click_id, "sumdep": sumdep, "tid": tenant_id, "secret": secret})
        return _err("bad_secret")

    new_count = (ua.total_deposits or 0) + 1
    async with SessionLocal() as s:
        await s.execute(UserAccess.__table__.update().where(UserAccess.id == ua.id).values(total_deposits=new_count))
        await s.commit()

    await _log_event("rd", ua, {"click_id": click_id, "sumdep": sumdep, "tid": tenant_id})

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
                .values(is_platinum=True, platinum_shown=False)  # <= сброс флага показа
            )
            await s.commit()

    await _push_next_screen(ua.id)
    return _ok(total_deposits=new_count, amount=sumdep)


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
