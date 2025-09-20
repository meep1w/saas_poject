import asyncio
from app.db import init_db
from app.bots.children_runner import run_children_loop

async def main():
    await init_db()
    await run_children_loop()

if __name__ == "__main__":
    asyncio.run(main())
