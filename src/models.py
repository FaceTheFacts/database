import time
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, Integer, Date, Boolean, ForeignKey, Float
from sqlalchemy.orm import session, relationship
from connection import Session, engine
from fetch import (
    constituency_fetch,
    country_fetch,
    city_fetch,
    election_program_fetch,
    electoral_list_fetch,
    fraction_fetch,
    parliament_fetch,
    parliament_period_fetch,
    party_fetch,
    politician_fetch,
)
from fetch import fetch_entity

import sys

sys.path.append("src")
from data.json_handler import json_fetch

Base = declarative_base()
session = Session()


class Country(Base):
    __tablename__ = "country"
    id = Column(Integer(), primary_key=True)
    entity_type = Column(String)
    label = Column(String)
    api_url = Column(String, unique=True)



def populate_countries() -> None:
    api_countries = fetch_entity("countries")
    session = Session()
    session.bulk_save_objects(
        [
            Country(
                id=api_country["id"],
                entity_type=api_country["entity_type"],
                label=api_country["label"],
                api_url=api_country["api_url"],
            )
            for api_country in api_countries
        ]
    )
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
    # One to Many
    candidacy_mandates = relationship("Candidacy_mandate", back_populates="party")


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
    # One to Many
    candidacy_mandates = relationship("Candidacy_mandate", back_populates="politician")


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
    # One to Many
    candidacy_mandates = relationship(
        "Candidacy_mandate", back_populates="parliament_period"
    )


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
    fraction_membership = relationship("Fraction_membership", back_populates="fraction")


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


class Constituency(Base):
    __tablename__ = "constituency"
    id = Column(Integer(), primary_key=True)
    entity_type = Column(String)
    label = Column(String)
    api_url = Column(String)
    name = Column(String)
    number = Column(Integer)
    parliament_period_id = Column(Integer, ForeignKey("parliament_period.id"))
    parliament_period = relationship("Parliament_period")
    electoral_data = relationship("Electoral_data", back_populates="constituency")


def isParliament_period():
    result = 0
    data = constituency_fetch()
    for datum in data:
        paliament_period = datum.get("paliament_period")
        if paliament_period:
            result += 1
    print(
        "Constituency included {result} 'paliament_period' in total out of {len_fetched_data} data".format(
            result=result, len_fetched_data=len(data)
        )
    )


def insert_constituency(data):
    data_list = []
    for datum in data:
        new_datum = Constituency(
            id=datum["id"],
            entity_type=datum["entity_type"],
            label=datum["label"],
            api_url=datum["api_url"],
            name=datum["name"],
            number=datum["number"],
        )
        data_list.append(new_datum)
    session.add_all(data_list)
    session.commit()
    print("Inserted {} data in total".format(len(data_list)))
    session.close()


class Electoral_list(Base):
    __tablename__ = "electoral_list"
    id = Column(Integer(), primary_key=True)
    entity_type = Column(String)
    label = Column(String)
    api_url = Column(String)
    name = Column(String)
    parliament_period_id = Column(Integer, ForeignKey("parliament_period.id"))
    parliament_period = relationship("Parliament_period")
    electoral_data = relationship("Electoral_data", back_populates="electoral_list")


def insert_electoral_list(data):
    data_list = []
    for datum in data:
        new_datum = Electoral_list(
            id=datum["id"],
            entity_type=datum["entity_type"],
            label=datum["label"],
            api_url=datum["api_url"],
            name=datum["name"],
            parliament_period_id=datum["parliament_period"]["id"]
            if datum["parliament_period"]
            else None,
        )
        data_list.append(new_datum)
    session.add_all(data_list)
    session.commit()
    print("Inserted {} data in total".format(len(data_list)))
    session.close()


def link_length_checker_election_program():
    data_list = []
    data = election_program_fetch()
    length_of_data = len(data)
    for datum in data:
        link = datum["link"]
        length_of_link = len(link)
        if length_of_link != 1:
            data_list.append(datum)

    print("Fetched {} data in total".format(length_of_data))
    print("{} has multiple links".format(data_list))


def link_checker_election_program():
    data_list = []
    data = election_program_fetch()
    length_of_data = len(data)
    for datum in data:
        id = datum["id"]
        link = datum["link"]
        uri = link[0]["uri"]
        if uri != None:
            hasUrl = {"id": id, "uri": uri}
            data_list.append(hasUrl)
    print("Fetched {} data in total".format(length_of_data))
    print("{} in total have uris".format(len(data_list)))


class Election_program(Base):
    __tablename__ = "election_program"
    id = Column(Integer(), primary_key=True)
    entity_type = Column(String)
    label = Column(String)
    api_url = Column(String)
    parliament_period_id = Column(Integer, ForeignKey("parliament_period.id"))
    party_id = Column(Integer, ForeignKey("party.id"))
    link_uri = Column(String)
    link_title = Column(String)
    link_option = Column(String)
    file = Column(String)
    parliament_period = relationship("Parliament_period")
    Party = relationship("Party")


def insert_election_program(data):
    data_list = []
    for datum in data:
        link = datum["link"][0]
        new_datum = Election_program(
            id=datum["id"],
            entity_type=datum["entity_type"],
            label=datum["label"],
            api_url=datum["api_url"],
            parliament_period_id=datum["parliament_period"]["id"]
            if datum["parliament_period"]
            else None,
            party_id=datum["party"]["id"] if datum["party"] else None,
            link_uri=link["uri"],
            link_title=link["title"],
            link_option=link["option"] if link.get("option") else None,
            file=datum["file"],
        )
        data_list.append(new_datum)
    session.add_all(data_list)
    session.commit()
    print("Inserted {} data in total".format(len(data_list)))
    session.close()


class Fraction_membership(Base):
    __tablename__ = "fraction_membership"
    id = Column(Integer, primary_key=True)
    entity_type = Column(String)
    label = Column(String)
    fraction_id = Column(Integer, ForeignKey("fraction.id"))
    valid_from = Column(String)
    valid_until = Column(String)
    fraction = relationship("Fraction", back_populates="fraction_membership")
    # One to One
    candidacy_mandate = relationship(
        "Candidacy_mandate", back_populates="fraction_membership"
    )


def insert_fraction_membership():
    data_list = []
    data = json_fetch("fraction_membership_fraction")
    for datum in data:
        new_datum = Fraction_membership(
            id=datum["id"],
            entity_type=datum["entity_type"],
            label=datum["label"],
            fraction_id=datum["fraction_id"],
            valid_from=datum["valid_from"],
            valid_until=datum["valid_until"],
        )
        data_list.append(new_datum)
    session.add_all(data_list)
    session.commit()
    print("Inserted {} data in total".format(len(data_list)))
    session.close()


class Electoral_data(Base):
    __tablename__ = "electoral_data"
    id = Column(Integer, primary_key=True)
    entity_type = Column(String)
    label = Column(String)
    electoral_list_id = Column(Integer, ForeignKey("electoral_list.id"))
    list_position = Column(Integer)
    constituency_id = Column(Integer, ForeignKey("constituency.id"))
    constituency_result = Column(Float)
    constituency_result_count = Column(Integer)
    mandate_won = Column(String)
    electoral_list = relationship("Electoral_list", back_populates="electoral_data")
    constituency = relationship("Constituency", back_populates="electoral_data")
    # One to One
    candidacy_mandate = relationship(
        "Candidacy_mandate", back_populates="electoral_data"
    )


def insert_electoral_data():
    data_list = []
    data = json_fetch("electoral_data_ids")
    for datum in data:
        new_datum = Electoral_data(
            id=datum["id"],
            entity_type=datum["entity_type"],
            label=datum["label"],
            electoral_list_id=datum["electoral_list_id"],
            list_position=datum["list_position"],
            constituency_id=datum["constituency_id"],
            constituency_result=datum["constituency_result"],
            constituency_result_count=datum["constituency_result_count"],
            mandate_won=datum["mandate_won"],
        )
        data_list.append(new_datum)
    session.add_all(data_list)
    session.commit()
    print("Inserted {} data in total".format(len(data_list)))
    session.close()


class Candidacy_mandate(Base):
    __tablename__ = "candidacy_mandate"
    id = Column(Integer, primary_key=True)
    entity_type = Column(String)
    label = Column(String)
    api_url = Column(String)
    id_external_administration = Column(String)
    id_external_administration_description = Column(String)
    type = Column(String)
    parliament_period_id = Column(Integer, ForeignKey("parliament_period.id"))
    politician_id = Column(Integer, ForeignKey("politician.id"))
    party_id = Column(Integer, ForeignKey("party.id"))
    start_date = Column(Date)
    end_date = Column(Date)
    info = Column(String)
    electoral_data_id = Column(Integer, ForeignKey("electoral_data.id"))
    fraction_membership_id = Column(Integer, ForeignKey("fraction_membership.id"))
    # Many to One
    parliament_period = relationship(
        "Parliament_period", back_populates="candidacy_mandates"
    )
    politician = relationship("Politician", back_populates="candidacy_mandates")
    party = relationship("Party", back_populates="candidacy_mandates")
    # One to One
    electoral_data = relationship(
        "Electoral_data", back_populates="candidacy_mandate", uselist=False
    )
    fraction_membership = relationship(
        "Fraction_membership", back_populates="candidacy_mandate", uselist=False
    )


def insert_candidacy_mandate():
    data_list = []
    data = json_fetch("candidacy_mandate_ids")
    for datum in data:
        new_datum = Candidacy_mandate(
            id=datum["id"],
            entity_type=datum["entity_type"],
            label=datum["label"],
            api_url=datum["api_url"],
            id_external_administration=datum["id_external_administration"],
            id_external_administration_description=datum[
                "id_external_administration_description"
            ],
            type=datum["type"],
            parliament_period_id=datum["parliament_period_id"],
            politician_id=datum["politician_id"],
            party_id=datum["party_id"],
            start_date=datum["start_date"],
            end_date=datum["end_date"],
            info=datum["info"],
            electoral_data_id=datum["electoral_data_id"],
            fraction_membership_id=datum["fraction_membership_id"],
        )
        data_list.append(new_datum)
    session.add_all(data_list)
    session.commit()
    print("Inserted {} data in total".format(len(data_list)))
    session.close()


if __name__ == "__main__":
    # Migration =>Table creation
    Base.metadata.create_all(engine)
    # populate_countries()
    # insert_city(city_fetch())
    # insert_party(party_fetch())
    # insert_politician(politician_fetch())
    # insert_parliament_period(parliament_period_fetch())
    # insert_parliament(parliament_fetch())
    # update_previous_period_id(parliament_period_fetch())
    # update_current_project_id(parliament_fetch())
    # insert_fraction(fraction_fetch())
    # isParliament_period()
    # insert_constituency(constituency_fetch())
    # insert_electoral_list(electoral_list_fetch())
    # link_length_checker_election_program()
    # link_checker_election_program()
    # insert_election_program(election_program_fetch())
    # insert_fraction_membership()
    # insert_electoral_data()
    insert_candidacy_mandate()
