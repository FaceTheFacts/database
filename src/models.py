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


def populate_cities() -> None:
    api_cities = fetch_entity("cities")
    session = Session()
    session.bulk_save_objects(
        [
            City(
                id=api_city["id"],
                entity_type=api_city["entity_type"],
                label=api_city["label"],
                api_url=api_city["api_url"],
            )
            for api_city in api_cities
        ]
    )
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


def populate_parties() -> None:
    api_parties = fetch_entity("parties")
    session = Session()
    session.bulk_save_objects(
        [
            Party(
                id=api_party["id"],
                entity_type=api_party["entity_type"],
                label=api_party["label"],
                api_url=api_party["api_url"],
                full_name=api_party["full_name"],
                short_name=api_party["short_name"],
            )
            for api_party in api_parties
        ]
    )
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


def populate_politicians() -> None:
    api_politicians = fetch_entity("politicians")

    time_begin = time.time()
    session = Session()
    session.bulk_save_objects(
        [
            Politician(
                id=api_politician["id"],
                entity_type=api_politician["entity_type"],
                label=api_politician["label"],
                api_url=api_politician["api_url"],
                abgeordnetenwatch_url=api_politician["abgeordnetenwatch_url"],
                first_name=api_politician["first_name"],
                last_name=api_politician["last_name"],
                birth_name=api_politician["birth_name"],
                sex=api_politician["sex"],
                year_of_birth=api_politician["year_of_birth"],
                party_id=api_politician["party"]["id"]
                if api_politician["party"]
                else None,
                party_past=api_politician["party_past"],
                deceased=api_politician["deceased"],
                deceased_date=api_politician["deceased_date"],
                education=api_politician["education"],
                residence=api_politician["residence"],
                occupation=api_politician["occupation"],
                statistic_questions=api_politician["statistic_questions"],
                statistic_questions_answered=api_politician[
                    "statistic_questions_answered"
                ],
                qid_wikidata=api_politician["qid_wikidata"],
                field_title=api_politician["field_title"],
            )
            for api_politician in api_politicians
        ]
    )
    session.commit()
    session.close()
    time_end = time.time()
    print(
        f"Total runtime to store {len(api_politicians)} rows for politicians is {time_end - time_begin}"
    )


class ParliamentPeriod(Base):
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


def populate_parliament_periods() -> None:
    api_parliament_periods = fetch_entity("parliament-periods")
    session = Session()
    session.bulk_save_objects(
        [
            ParliamentPeriod(
                id=api_parliament_period["id"],
                entity_type=api_parliament_period["entity_type"],
                label=api_parliament_period["label"],
                api_url=api_parliament_period["api_url"],
                abgeordnetenwatch_url=api_parliament_period["abgeordnetenwatch_url"],
                type=api_parliament_period["type"],
                election_date=api_parliament_period["election_date"],
                start_date_period=api_parliament_period["start_date_period"],
                end_date_period=api_parliament_period["end_date_period"],
                previous_period_id=api_parliament_period["previous_period"]["id"]
                if api_parliament_period["previous_period"]
                else None,
                parliament_id=api_parliament_period["parliament"]["id"]
                if api_parliament_period["parliament"]
                else None,
            )
            for api_parliament_period in api_parliament_periods
        ]
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
    current_project_id = Column(Integer, ForeignKey("parliament_period.id"))


def populate_parliaments() -> None:
    api_parliaments = fetch_entity("parliaments")
    session = Session()
    session.bulk_save_objects(
        [
            Parliament(
                id=api_parliament["id"],
                entity_type=api_parliament["entity_type"],
                label=api_parliament["label"],
                api_url=api_parliament["api_url"],
                abgeordnetenwatch_url=api_parliament["abgeordnetenwatch_url"],
                label_external_long=api_parliament["label_external_long"],
                current_project_id=api_parliament["current_project"]["id"]
                if api_parliament["current_project"]
                else None,
            )
            for api_parliament in api_parliaments
        ]
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
    parliament_period = relationship("ParliamentPeriod")
    fraction_membership = relationship("Fraction_membership", back_populates="fraction")


def populate_fractions() -> None:
    api_fractions = fetch_entity("fractions")
    session = Session()
    session.bulk_save_objects(
        [
            Fraction(
                id=api_fraction["id"],
                entity_type=api_fraction["entity_type"],
                label=api_fraction["label"],
                api_url=api_fraction["api_url"],
                full_name=api_fraction["full_name"],
                short_name=api_fraction["short_name"],
                legislature_id=api_fraction["legislature"]["id"]
                if api_fraction["legislature"]
                else None,
            )
            for api_fraction in api_fractions
        ]
    )
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
    electoral_data = relationship("Electoral_data", back_populates="constituency")


def populate_constituencies():
    api_constituencies = fetch_entity("constituencies")
    session = Session()
    session.bulk_save_objects(
        [
            Constituency(
                id=api_constituency["id"],
                entity_type=api_constituency["entity_type"],
                label=api_constituency["label"],
                api_url=api_constituency["api_url"],
                name=api_constituency["name"],
                number=api_constituency["number"],
                parliament_period_id=api_constituency["parliament_period"]["id"]
                if api_constituency["parliament_period"]
                else None,
            )
            for api_constituency in api_constituencies
        ]
    )
    session.commit()
    session.close()


class ElectoralList(Base):
    __tablename__ = "electoral_list"
    id = Column(Integer(), primary_key=True)
    entity_type = Column(String)
    label = Column(String)
    api_url = Column(String)
    name = Column(String)
    parliament_period_id = Column(Integer, ForeignKey("parliament_period.id"))
    parliament_period = relationship("ParliamentPeriod")
    electoral_data = relationship("Electoral_data", back_populates="electoral_list")


def insert_electoral_list(data):
    data_list = []
    for datum in data:
        new_datum = ElectoralList(
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
    parliament_period = relationship("ParliamentPeriod")
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
    electoral_list = relationship("ElectoralList", back_populates="electoral_data")
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
        "ParliamentPeriod", back_populates="candidacy_mandates"
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
    # populate_cities()
    # populate_parties()
    # populate_politicians()
    # populate_parliament_periods()
    # populate_parliaments()
    # populate_fractions()
    # populate_constituencies()
    # insert_electoral_list(electoral_list_fetch())
    # link_length_checker_election_program()
    # link_checker_election_program()
    # insert_election_program(election_program_fetch())
    # insert_fraction_membership()
    # insert_electoral_data()
    insert_candidacy_mandate()
