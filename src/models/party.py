from sqlalchemy import Column, String, Integer
from sqlalchemy.orm import relationship

from ..db.base import Base


class Party(Base):
    __tablename__ = "party"

    id = Column(Integer(), primary_key=True)
    entity_type = Column(String)
    label = Column(String)
    api_url = Column(String, unique=True)
    full_name = Column(String)
    short_name = Column(String)

    # One to Many
    candidacy_mandates = relationship("CandidacyMandate", back_populates="party")
