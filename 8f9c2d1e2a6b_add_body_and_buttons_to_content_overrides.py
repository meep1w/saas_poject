"""add body_html and buttons_json to content_overrides

Revision ID: 8f9c2d1e2a6b
Revises: REPLACE_WITH_YOUR_PREV_REVISION
Create Date: 2025-10-05

"""
from alembic import op
import sqlalchemy as sa

# если у вас PostgreSQL — возьмем JSONB
try:
    from sqlalchemy.dialects import postgresql
except Exception:  # на всякий случай
    postgresql = None

# --- Alembic identifiers
revision = "8f9c2d1e2a6b"
down_revision = "REPLACE_WITH_YOUR_PREV_REVISION"  # <-- замените на актуальный предыдущий ревизионный id
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    dialect = bind.dialect.name if bind is not None else None

    # Подберем тип для колонок с учетом диалекта
    if dialect == "postgresql" and postgresql is not None:
        json_type = postgresql.JSONB(astext_type=sa.Text())
    else:
        json_type = sa.JSON()

    # Добавляем колонки, если их ещё нет
    # В Alembic обычно не используют "IF NOT EXISTS"; полагаемся на историю миграций.
    op.add_column(
        "content_overrides",
        sa.Column("body_html", sa.Text(), nullable=True),
    )
    op.add_column(
        "content_overrides",
        sa.Column("buttons_json", json_type, nullable=True),
    )


def downgrade():
    # Откат: просто удаляем колонки
    with op.batch_alter_table("content_overrides") as batch_op:
        try:
            batch_op.drop_column("buttons_json")
        except Exception:
            pass
        try:
            batch_op.drop_column("body_html")
        except Exception:
            pass
