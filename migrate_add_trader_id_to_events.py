# migrate_add_trader_id_to_events.py
import sqlite3

db_path = "saas.db"

with sqlite3.connect(db_path) as conn:
    cur = conn.cursor()
    # Добавляем колонку, если её нет
    cur.execute("PRAGMA table_info(events)")
    cols = {row[1] for row in cur.fetchall()}
    if "trader_id" not in cols:
        cur.execute("ALTER TABLE events ADD COLUMN trader_id VARCHAR(64)")
        # индекс опционально, но полезен для поиска/аналитики
        cur.execute("CREATE INDEX IF NOT EXISTS ix_events_trader_id ON events(trader_id)")
    conn.commit()

print("Done: events.trader_id added (if it was missing).")
