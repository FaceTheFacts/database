"""create the constituency table

Revision ID: 453aef5c78aa
Revises: cfa823e6cc5f
Create Date: 2021-10-04 16:25:08.441483

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "453aef5c78aa"
down_revision = "cfa823e6cc5f"
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table(
        "constituency",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("entity_type", sa.String(), nullable=True),
        sa.Column("label", sa.String(), nullable=True),
        sa.Column("api_url", sa.String(), nullable=True),
        sa.Column("name", sa.String(), nullable=True),
        sa.Column("number", sa.Integer(), nullable=True),
        sa.Column("parliament_period_id", sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(
            ["parliament_period_id"],
            ["parliament_period.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column(
        "parliament",
        sa.Column(
            "current_project_id", sa.INTEGER(), autoincrement=False, nullable=True
        ),
    )
    op.create_foreign_key(
        "parliament_current_project_id_fkey",
        "parliament",
        "parliament_period",
        ["current_project_id"],
        ["id"],
    )
    # ### end Alembic commands ###
