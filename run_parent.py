import asyncio
from app.db import init_db
from app.bots.parent_bot import run_parent

async def main():
    await init_db()
    await run_parent()

if __name__ == "__main__":
    asyncio.run(main())
