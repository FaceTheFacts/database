import time
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, Integer, Date, Boolean, ForeignKey
from sqlalchemy.orm import session, relationship
from connection import Session, engine
from fetch import country_fetch, city_fetch, party_fetch, politician_fetch

Base = declarative_base()
session = Session()


class Country(Base):
    __tablename__ = "country"
    id = Column(Integer(), primary_key=True)
    entity_type = Column(String)
    label = Column(String)
    api_url = Column(String, unique=True)


def insert_country(data: list):
    data_list = []
    for datum in data:
        new_country = Country(
            id=datum["id"],
            entity_type=datum["entity_type"],
            label=datum["label"],
            api_url=datum["api_url"],
        )
        data_list.append(new_country)
    session.add_all(data_list)
    session.commit()
    session.close()


class City(Base):
    __tablename__ = "city"
    id = Column(Integer(), primary_key=True)
    entity_type = Column(String)
    label = Column(String)
    api_url = Column(String, unique=True)


def insert_city(data: list):
    data_list = []
    for datum in data:
        new_datum = City(
            id=datum["id"],
            entity_type=datum["entity_type"],
            label=datum["label"],
            api_url=datum["api_url"],
        )
        data_list.append(new_datum)
    session.add_all(data_list)
    session.commit()
    session.close()


class Party(Base):
    __tablename__ = "party"
    id = Column(Integer(), primary_key=True)
    entity_type = Column(String)
    label = Column(String)
    api_url = Column(String, unique=True)
    full_name = Column(String)
    short_name = Column(String)


def insert_party(data: list):
    # party No.26 is missing
    data_list = []
    for datum in data:
        new_datum = Party(
            id=datum["id"],
            entity_type=datum["entity_type"],
            label=datum["label"],
            api_url=datum["api_url"],
            full_name=datum["full_name"],
            short_name=datum["short_name"],
        )
        data_list.append(new_datum)
    session.add_all(data_list)
    session.commit()
    session.close()


class Politician(Base):
    __tablename__ = "politician"
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


def insert_politician(data: list):
    begin = time.time()
    data_list = []
    for datum in data:
        new_datum = Politician(
            id=datum["id"],
            entity_type=datum["entity_type"],
            label=datum["label"],
            api_url=datum["api_url"],
            abgeordnetenwatch_url=datum["abgeordnetenwatch_url"],
            first_name=datum["first_name"],
            last_name=datum["last_name"],
            birth_name=datum["birth_name"],
            sex=datum["sex"],
            year_of_birth=datum["year_of_birth"],
            party_id=datum["party"]["id"] if datum["party"] else None,
            party_past=datum["party_past"],
            deceased=datum["deceased"],
            deceased_date=datum["deceased_date"],
            education=datum["education"],
            residence=datum["residence"],
            occupation=datum["occupation"],
            statistic_questions=datum["statistic_questions"],
            statistic_questions_answered=datum["statistic_questions_answered"],
            qid_wikidata=datum["qid_wikidata"],
            field_title=datum["field_title"],
        )
        data_list.append(new_datum)
    session.add_all(data_list)
    session.commit()
    session.close()
    end = time.time()
    print(f"Total runtime to store {len(data_list)} data is {end - begin}")


if __name__ == "__main__":
    # Migration =>Table creation
    Base.metadata.create_all(engine)
    # insert_country(country_fetch())
    # insert_city(city_fetch())
    # insert_party(party_fetch())
    # insert_politician(politician_fetch())
