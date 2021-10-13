from sqlalchemy import Column, String, Integer, ForeignKey, BigInteger
from sqlalchemy.orm import relationship

from ..db.base import Base


class Position(Base):
    __tablename__ = "position"

    # id has the following structure parliament_period + statement_number (130 + 1 -> 1301)
    id = Column(BigInteger, primary_key=True)
    position = Column(String)
    reason = Column(String())
    politician_id = Column(Integer, ForeignKey("politician.id"))
    parliament_period_id = Column(Integer, ForeignKey("parliament_period.id"))
    position_statement_id = Column(Integer, ForeignKey("position_statement.id"))
    politician = relationship("Politician", back_populates="positions")
    parliament_periods = relationship("Parliament_period", back_populates="positions")
    position_statements = relationship("Position_statement", back_populates="positions")
