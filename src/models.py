from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, Integer
from sqlalchemy.orm import session
from connection import Session, engine
from fetch import country_fetch, city_fetch, party_fetch

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


if __name__ == "__main__":
    # Migration
    Base.metadata.create_all(engine)
    # insert_country(country_fetch())
    # insert_city(city_fetch())
    insert_party(party_fetch())
