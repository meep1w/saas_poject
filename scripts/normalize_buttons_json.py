# scripts/normalize_buttons_json.py
import asyncio
import json
from typing import Any

from app.db import SessionLocal
from app.models import ContentOverride

def to_dict_or_none(v: Any):
    # Уже dict — ок
    if isinstance(v, dict):
        return v
    # Попытка распарсить строку как JSON
    if isinstance(v, str):
        try:
            x = json.loads(v)
        except Exception:
            return None
        # Если получилась строка с ещё одним уровнем JSON — распарсим ещё раз
        if isinstance(x, str):
            try:
                x2 = json.loads(x)
                return x2 if isinstance(x2, dict) else None
            except Exception:
                return None
        return x if isinstance(x, dict) else None
    # Всё остальное — в корзину
    return None

async def main():
    fixed = 0
    nulled = 0
    kept = 0
    async with SessionLocal() as s:
        res = await s.execute(ContentOverride.__table__.select())
        rows = res.fetchall()

        for row in rows:
            row_id = row.id
            raw = row.buttons_json

            # Нормализуем
            new_val = to_dict_or_none(raw)

            # Ничего менять не нужно
            if (isinstance(raw, dict) and new_val == raw):
                kept += 1
                continue

            # Пишем dict либо NULL
            if new_val is not None:
                await s.execute(
                    ContentOverride.__table__
                    .update()
                    .where(ContentOverride.id == row_id)
                    .values(buttons_json=new_val)
                )
                fixed += 1
            else:
                await s.execute(
                    ContentOverride.__table__
                    .update()
                    .where(ContentOverride.id == row_id)
                    .values(buttons_json=None)
                )
                nulled += 1

        await s.commit()

    print(f"Готово. Исправлено: {fixed}, обнулено: {nulled}, без изменений: {kept}")

if __name__ == "__main__":
    asyncio.run(main())
