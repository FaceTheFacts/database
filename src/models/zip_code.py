from sqlalchemy import Column, Integer, ForeignKey
from sqlalchemy.orm import relationship

from ..db.base import Base


class ZipCode(Base):
    __tablename__ = "zip_code"

    id = Column(Integer, primary_key=True),
    constituency_id = Column(Integer, ForeignKey("constituency.id")),
    zip_code = Column(Integer)

    # Many to One
    constituency = relationship("Constituency", back_populates="zip_codes")