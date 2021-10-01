import sys

sys.path.append("src")

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, Integer, ForeignKey, Date, Boolean
from sqlalchemy.orm import relationship
from connection import engine

Base = declarative_base()


class Politician(Base):
    __tablename__ = "politician_two"
    id = Column(Integer(), primary_key=True)
    entity_type = Column(String)
    label = Column(String)
    api_url = Column(String)
    abgeordnetenwatch_url = Column(String)
    first_name = Column(String)
    last_name = Column(String)
    birth_name = Column(String)
    sex = Column(String)
    year_of_birth = Column(String)
    party_id = Column(Integer, ForeignKey("party.id"))
    party_past = Column(String)
    deceased = Column(Boolean)
    deceased_date = Column(Date)
    education = Column(String)
    residence = Column(String)
    occupation = Column(String)
    statistic_questions = Column(String)
    statistic_questions_answered = Column(String)
    qid_wikidata = Column(String)
    field_title = Column(String)
    party = relationship("Party")


# Migrate Table
Base.metadata.create_all(engine)
