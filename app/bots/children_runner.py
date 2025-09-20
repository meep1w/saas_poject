import asyncio
from typing import Dict
from sqlalchemy import select
from app.db import SessionLocal
from app.models import Tenant
from app.bots.child.bot_instance import run_child_bot
from app.utils.logging import logger

class ChildrenManager:
    def __init__(self):
        self.tasks: Dict[int, asyncio.Task] = {}

    async def tick(self):
        async with SessionLocal() as s:
            res = await s.execute(select(Tenant).where(Tenant.is_active == True))
            tenants = res.scalars().all()
        for t in tenants:
            if t.id not in self.tasks:
                logger.info(f"Starting child bot for tenant {t.id} @ {t.bot_username}")
                self.tasks[t.id] = asyncio.create_task(run_child_bot(t.bot_token, t.id))

        # TODO: detect token changes/deactivation and restart tasks

async def run_children_loop():
    manager = ChildrenManager()
    while True:
        try:
            await manager.tick()
        except Exception as e:
            logger.exception(f"Tick error: {e}")
        await asyncio.sleep(2)
