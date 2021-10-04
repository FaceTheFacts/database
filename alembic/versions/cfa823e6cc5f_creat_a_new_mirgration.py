"""creat a new mirgration

Revision ID: cfa823e6cc5f
Revises: 
Create Date: 2021-10-03 11:39:42.969959

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "cfa823e6cc5f"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "parliament_period",
        sa.Column("parliament_id", sa.INTEGER, sa.ForeignKey("parliament.id")),
    )
    op.add_column(
        "parliament_period",
        sa.Column(
            "previous_period_id", sa.INTEGER, sa.ForeignKey("parliament_period.id")
        ),
    )

    op.add_column(
        "parliament",
        sa.Column(
            "current_project_id", sa.INTEGER, sa.ForeignKey("parliament_period.id")
        ),
    )


def downgrade():
    pass
