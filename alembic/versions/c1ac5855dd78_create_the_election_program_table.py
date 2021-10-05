"""create the election_program table

Revision ID: c1ac5855dd78
Revises: c0675a9a02fc
Create Date: 2021-10-05 10:39:45.955409

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "c1ac5855dd78"
down_revision = "c0675a9a02fc"
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table(
        "election_program",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("entity_type", sa.String(), nullable=True),
        sa.Column("label", sa.String(), nullable=True),
        sa.Column("api_url", sa.String(), nullable=True),
        sa.Column("parliament_period_id", sa.Integer(), nullable=True),
        sa.Column("party_id", sa.Integer(), nullable=True),
        sa.Column("link_uri", sa.String(), nullable=True),
        sa.Column("link_title", sa.String(), nullable=True),
        sa.Column("link_option", sa.String(), nullable=True),
        sa.Column("file", sa.String(), nullable=True),
        sa.ForeignKeyConstraint(
            ["parliament_period_id"],
            ["parliament_period.id"],
        ),
        sa.ForeignKeyConstraint(
            ["party_id"],
            ["party.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table("election_program")
    # ### end Alembic commands ###
