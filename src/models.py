import time
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, Integer, Date, Boolean, ForeignKey
from sqlalchemy.orm import session, relationship
from connection import Session, engine
from fetch import (
    country_fetch,
    city_fetch,
    fraction_fetch,
    parliament_fetch,
    parliament_period_fetch,
    party_fetch,
    politician_fetch,
)

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


class Parliament_period(Base):
    __tablename__ = "parliament_period"
    id = Column(Integer(), primary_key=True)
    entity_type = Column(String)
    label = Column(String)
    api_url = Column(String)
    abgeordnetenwatch_url = Column(String)
    type = Column(String)
    election_date = Column(Date)
    start_date_period = Column(Date)
    end_date_period = Column(Date)
    parliament_id = Column(Integer, ForeignKey("parliament.id"))
    previous_period_id = Column(Integer, ForeignKey("parliament_period.id"))
    parliament = relationship("Parliament")


def insert_parliament_period(data: list):
    data_list = []
    for datum in data:
        new_datum = Parliament_period(
            id=datum["id"],
            entity_type=datum["entity_type"],
            label=datum["label"],
            api_url=datum["api_url"],
            abgeordnetenwatch_url=datum["abgeordnetenwatch_url"],
            type=datum["type"],
            election_date=datum["election_date"],
            start_date_period=datum["start_date_period"],
            end_date_period=datum["end_date_period"],
            parliament_id=datum["parliament"]["id"] if datum["parliament"] else None,
            # previous_period_id=datum["previous_period"]["id"] if datum["previous_period"] else None,
        )
        data_list.append(new_datum)
    session.add_all(data_list)
    session.commit()
    session.close()


def update_previous_period_id(data: list):
    data_list = []
    for datum in data:
        new_data = {
            "id": datum["id"],
            "previous_period_id": datum["previous_period"]["id"]
            if datum["previous_period"]
            else None,
        }
        data_list.append(new_data)

    for data_dict in data_list:
        if data_dict["previous_period_id"] != None:
            engine.execute(
                "UPDATE {table} SET previous_period_id = {previous_period_id} WHERE id = {id}".format(
                    table=Parliament_period.__tablename__,
                    previous_period_id=data_dict["previous_period_id"],
                    id=data_dict["id"],
                )
            )
    session.commit()
    session.close()


class Parliament(Base):
    __tablename__ = "parliament"
    id = Column(Integer(), primary_key=True)
    entity_type = Column(String)
    label = Column(String)
    api_url = Column(String)
    abgeordnetenwatch_url = Column(String)
    label_external_long = Column(String)
    # current_project_id = Column(Integer, ForeignKey("parliament_period.id"))
    # parliament_period = relationship("Parliament_period")


def insert_parliament(data: list):
    data_list = []
    for datum in data:
        new_datum = Parliament(
            id=datum["id"],
            entity_type=datum["entity_type"],
            label=datum["label"],
            api_url=datum["api_url"],
            abgeordnetenwatch_url=datum["abgeordnetenwatch_url"],
            label_external_long=datum["label_external_long"],
        )
        data_list.append(new_datum)
    session.add_all(data_list)
    session.commit()
    session.close()


def update_current_project_id(data: list):
    data_list = []
    for datum in data:
        new_data = {
            "id": datum["id"],
            "current_project_id": datum["current_project"]["id"]
            if datum["current_project"]
            else None,
        }
        data_list.append(new_data)

    for data_dict in data_list:
        if data_dict["current_project_id"] != None:
            engine.execute(
                "UPDATE {table} SET current_project_id = {current_project_id} WHERE id = {id}".format(
                    table=Parliament.__tablename__,
                    current_project_id=data_dict["current_project_id"],
                    id=data_dict["id"],
                )
            )
    session.commit()
    session.close()


class Fraction(Base):
    __tablename__ = "fraction"
    id = Column(Integer(), primary_key=True)
    entity_type = Column(String)
    label = Column(String)
    api_url = Column(String)
    full_name = Column(String)
    short_name = Column(String)
    legislature_id = Column(Integer, ForeignKey("parliament_period.id"))
    parliament_period = relationship("Parliament_period")


def insert_fraction(data: list) -> None:
    data_list = []
    for datum in data:
        new_datum = Fraction(
            id=datum["id"],
            entity_type=datum["entity_type"],
            label=datum["label"],
            api_url=datum["api_url"],
            full_name=datum["full_name"],
            short_name=datum["short_name"],
            legislature_id=datum["legislature"]["id"] if datum["legislature"] else None,
        )
        data_list.append(new_datum)
    session.add_all(data_list)
    session.commit()
    session.close()


if __name__ == "__main__":
    # Migration =>Table creation
    Base.metadata.create_all(engine)
    # insert_country(country_fetch())
    # insert_city(city_fetch())
    # insert_party(party_fetch())
    # insert_politician(politician_fetch())
    # insert_parliament_period(parliament_period_fetch())
    # insert_parliament(parliament_fetch())
    # update_previous_period_id(parliament_period_fetch())
    # update_current_project_id(parliament_fetch())
    insert_fraction(fraction_fetch())
