import time
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import (
    Column,
    String,
    Integer,
    Date,
    Boolean,
    ForeignKey,
    BigInteger,
    Float,
    select,
    Text,
)
from sqlalchemy.orm import session, relationship
from connection import Session, engine
from fetch import (
    committee_fetch,
    constituency_fetch,
    electoral_list_fetch,
    fraction_fetch,
    topic_fetch,
)
import json
from fetch import load_entity
from utils import read_json
from parser import get_electoral_data_list

import sys

sys.path.append("src")
from data.json_handler import (
    cv_json_fetch,
    cv_json_file_numbers_generator,
    json_fetch,
    json_generator,
)

Base = declarative_base()
session = Session()


class Country(Base):
    __tablename__ = "country"
    id = Column(Integer(), primary_key=True)
    entity_type = Column(String)
    label = Column(String)
    api_url = Column(String, unique=True)
    # One to Many
    sidejob_organizations = relationship(
        "SidejobOrganization", back_populates="country"
    )
    # One to Many
    sidejobs = relationship("Sidejob", back_populates="country")


def populate_countries() -> None:
    api_countries = load_entity("countries")
    countries = [
        {
            "id": api_country["id"],
            "entity_type": api_country["entity_type"],
            "label": api_country["label"],
            "api_url": api_country["api_url"],
        }
        for api_country in api_countries
    ]
    session = Session()
    stmt = insert(Country).values(countries)
    stmt = stmt.on_conflict_do_update(
        constraint="country_pkey",
        set_={col.name: col for col in stmt.excluded if not col.primary_key},
    )
    session.execute(stmt)
    session.commit()
    session.close()


class City(Base):
    __tablename__ = "city"
    id = Column(Integer(), primary_key=True)
    entity_type = Column(String)
    label = Column(String)
    api_url = Column(String, unique=True)
    # One to Many
    sidejob_organizations = relationship("SidejobOrganization", back_populates="city")
    # One to Many
    sidejobs = relationship("Sidejob", back_populates="city")


def populate_cities() -> None:
    api_cities = load_entity("cities")
    cities = [
        {
            "id": api_city["id"],
            "entity_type": api_city["entity_type"],
            "label": api_city["label"],
            "api_url": api_city["api_url"],
        }
        for api_city in api_cities
    ]
    session = Session()
    stmt = insert(City).values(cities)
    stmt = stmt.on_conflict_do_update(
        constraint="city_pkey",
        set_={col.name: col for col in stmt.excluded if not col.primary_key},
    )
    session.execute(stmt)
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
    candidacy_mandates = relationship("CandidacyMandate", back_populates="party")


def populate_parties() -> None:
    api_parties = load_entity("parties")
    parties = [
        {
            "id": api_party["id"],
            "entity_type": api_party["entity_type"],
            "label": api_party["label"],
            "api_url": api_party["api_url"],
            "full_name": api_party["full_name"],
            "short_name": api_party["short_name"],
        }
        for api_party in api_parties
    ]
    session = Session()
    stmt = insert(Party).values(parties)
    stmt = stmt.on_conflict_do_update(
        constraint="party_pkey",
        set_={col.name: col for col in stmt.excluded if not col.primary_key},
    )
    session.execute(stmt)
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
    candidacy_mandates = relationship("CandidacyMandate", back_populates="politician")
    positions = relationship("Position", back_populates="politicians")


def populate_politicians() -> None:
    api_politicians = load_entity("politicians")
    begin_time = time.time()
    politicians = [
        {
            "id": api_politician["id"],
            "entity_type": api_politician["entity_type"],
            "label": api_politician["label"],
            "api_url": api_politician["api_url"],
            "abgeordnetenwatch_url": api_politician["abgeordnetenwatch_url"],
            "first_name": api_politician["first_name"],
            "last_name": api_politician["last_name"],
            "birth_name": api_politician["birth_name"],
            "sex": api_politician["sex"],
            "year_of_birth": api_politician["year_of_birth"],
            "party_id": api_politician["party"]["id"]
            if api_politician["party"]
            else None,
            "party_past": api_politician["party_past"],
            "deceased": api_politician["deceased"],
            "deceased_date": api_politician["deceased_date"],
            "education": api_politician["education"],
            "residence": api_politician["residence"],
            "occupation": api_politician["occupation"],
            "statistic_questions": api_politician["statistic_questions"],
            "statistic_questions_answered": api_politician[
                "statistic_questions_answered"
            ],
            "qid_wikidata": api_politician["qid_wikidata"],
            "field_title": api_politician["field_title"],
        }
        for api_politician in api_politicians
    ]
    stmt = insert(Politician).values(politicians)
    stmt = stmt.on_conflict_do_update(
        constraint="politician_pkey",
        set_={col.name: col for col in stmt.excluded if not col.primary_key},
    )
    session = Session()
    session.execute(stmt)
    session.commit()
    session.close()
    end_time = time.time()
    print(
        f"Total runtime to store {len(api_politicians)} data is {end_time - begin_time}"
    )


class Parliament(Base):
    __tablename__ = "parliament"
    id = Column(Integer(), primary_key=True)
    entity_type = Column(String)
    label = Column(String)
    api_url = Column(String)
    abgeordnetenwatch_url = Column(String)
    label_external_long = Column(String)

    # current_project_id = Column(Integer, ForeignKey("parliament_period.id"))
    # parliament_period = relationship("ParliamentPeriod")


def populate_parliaments() -> None:
    api_parliaments = load_entity("parliaments")
    parliaments = [
        {
            "id": api_parliament["id"],
            "entity_type": api_parliament["entity_type"],
            "label": api_parliament["label"],
            "api_url": api_parliament["api_url"],
            "abgeordnetenwatch_url": api_parliament["abgeordnetenwatch_url"],
            "label_external_long": api_parliament["label_external_long"],
        }
        for api_parliament in api_parliaments
    ]
    session = Session()
    stmt = insert(Parliament).values(parliaments)
    stmt = stmt.on_conflict_do_update(
        constraint="parliament_pkey",
        set_={col.name: col for col in stmt.excluded if not col.primary_key},
    )
    session.execute(stmt)
    session.commit()
    session.close()


# def update_parliament_project():
#     session = Session()
#     api_parliaments = load_entity("parliaments")

#     foo = []
#     for ap in api_parliaments:
#         p = {
#             "id": ap["id"],
#             "current_project_id": ap["current_project"]["id"]
#             if ap["current_project"]
#             else None,
#         }
#         foo.append(p)

#     # print(foo)
#     session.bulk_update_mappings(Parliament, foo)
#     session.commit()


def update_parliament_current_project_ids():
    api_parliaments = load_entity("parliaments")
    parliaments = [
        {
            "id": parliament["id"],
            "current_project_id": parliament["current_project"]["id"]
            if parliament["current_project"]
            else None,
        }
        for parliament in api_parliaments
    ]

    for parliament in parliaments:
        if parliament["current_project_id"]:
            engine.execute(
                "UPDATE {table} SET current_project_id = {current_project_id} WHERE id = {id}".format(
                    table=Parliament.__tablename__,
                    current_project_id=parliament["current_project_id"],
                    id=parliament["id"],
                )
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
        "CandidacyMandate", back_populates="parliament_period"
    )
    polls = relationship("Poll", back_populates="parliament_period")

    positions = relationship("Position", back_populates="parliament_periods")


def populate_parliament_periods():
    api_parliament_periods = load_entity("parliament-periods")
    parliament_periods = [
        {
            "id": api_parliament_period["id"],
            "entity_type": api_parliament_period["entity_type"],
            "label": api_parliament_period["label"],
            "api_url": api_parliament_period["api_url"],
            "abgeordnetenwatch_url": api_parliament_period["abgeordnetenwatch_url"],
            "type": api_parliament_period["type"],
            "election_date": api_parliament_period["election_date"],
            "start_date_period": api_parliament_period["start_date_period"],
            "end_date_period": api_parliament_period["end_date_period"],
            "parliament_id": api_parliament_period["parliament"]["id"]
            if api_parliament_period["parliament"]
            else None,
            "previous_period_id": api_parliament_period["previous_period"]["id"]
            if api_parliament_period["previous_period"]
            else None,
        }
        for api_parliament_period in api_parliament_periods
    ]
    parliament_periods = sorted(parliament_periods, key=lambda p: p["id"])
    stmt = insert(ParliamentPeriod).values(parliament_periods)
    stmt = stmt.on_conflict_do_update(
        constraint="parliament_period_pkey",
        set_={col.name: col for col in stmt.excluded if not col.primary_key},
    )
    session = Session()
    session.execute(stmt)
    session.commit()

    update_parliament_current_project_ids()
    session.close()


class Topic(Base):
    __tablename__ = "topic"
    id = Column(Integer, primary_key=True)
    entity_type = Column(String)
    label = Column(String)
    api_url = Column(String)
    abgeordnetenwatch_url = Column(String)
    description = Column(String)
    parent_id = Column(Integer(), ForeignKey("topic.id"))
    committees = relationship(
        "Committee", secondary="committee_has_topic", back_populates="topics"
    )
    # Many to Many
    polls = relationship("Poll", secondary="poll_has_topic", back_populates="topics")
    sidejob_organizations = relationship(
        "SidejobOrganization",
        secondary="sidejob_organization_has_topic",
        back_populates="topics",
    )
    # Many to Many
    sidejobs = relationship(
        "Sidejob",
        secondary="sidejob_has_topic",
        back_populates="topics",
    )
    position_statements = relationship("Position_statement", back_populates="topics")


def populate_topics():
    api_topics = load_entity("topics")
    topics = [
        {
            "id": api_topic["id"],
            "entity_type": api_topic["entity_type"],
            "label": api_topic["label"],
            "api_url": api_topic["api_url"],
            "abgeordnetenwatch_url": api_topic["abgeordnetenwatch_url"],
            "description": api_topic["description"],
            "parent_id": api_topic["parent"][0]["id"] if api_topic["parent"] else None,
        }
        for api_topic in api_topics
    ]
    topics = sorted(topics, key=lambda t: t["id"])
    stmt = insert(Topic).values(topics)
    stmt = stmt.on_conflict_do_update(
        constraint="topic_pkey",
        set_={col.name: col for col in stmt.excluded if not col.primary_key},
    )
    session = Session()
    session.execute(stmt)
    session.commit()
    session.close()


class Committee(Base):
    __tablename__ = "committee"
    id = Column(Integer(), primary_key=True)
    entity_type = Column(String)
    label = Column(String)
    api_url = Column(String)
    field_legislature_id = Column(Integer(), ForeignKey("parliament_period.id"))
    parliament_period = relationship("ParliamentPeriod", backref="parliament_period")
    topics = relationship(
        "Topic", secondary="committee_has_topic", back_populates="committees"
    )
    # One to Many
    committee_memberships = relationship(
        "CommitteeMembership", back_populates="committee"
    )
    polls = relationship("Poll", back_populates="committee")


def populate_committees():
    api_committees = load_entity("committees")
    committees = [
        {
            "id": api_committee["id"],
            "entity_type": api_committee["entity_type"],
            "label": api_committee["label"],
            "api_url": api_committee["api_url"],
            "field_legislature_id": api_committee["field_legislature"]["id"],
        }
        for api_committee in api_committees
    ]
    stmt = insert(Committee).values(committees)
    stmt = stmt.on_conflict_do_update(
        constraint="committee_pkey",
        set_={col.name: col for col in stmt.excluded if not col.primary_key},
    )
    session = Session()
    session.execute(stmt)
    session.commit()
    session.close()


class CommitteeHasTopic(Base):
    __tablename__ = "committee_has_topic"
    committee_id = Column(Integer(), ForeignKey("committee.id"), primary_key=True)
    topic_id = Column(Integer(), ForeignKey("topic.id"), primary_key=True)


def populate_committee_has_topic() -> None:
    api_committees = load_entity("committees")
    committee_topics = []
    for api_committee in api_committees:
        field_topics = api_committee["field_topics"]
        if field_topics:
            for topic in field_topics:
                committee_topic = {
                    "committee_id": api_committee["id"],
                    "topic_id": topic["id"],
                }
                committee_topics.append(committee_topic)
    stmt = insert(CommitteeHasTopic).values(committee_topics)
    stmt = stmt.on_conflict_do_nothing()
    session = Session()
    session.execute(stmt)
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
    fraction_membership = relationship("FractionMembership", back_populates="fraction")
    # One to Many
    votes = relationship("Vote", back_populates="fraction")


def populate_fractions() -> None:
    api_fractions = load_entity("fractions")
    fractions = [
        {
            "id": api_fraction["id"],
            "entity_type": api_fraction["entity_type"],
            "label": api_fraction["label"],
            "api_url": api_fraction["api_url"],
            "full_name": api_fraction["full_name"],
            "short_name": api_fraction["short_name"],
            "legislature_id": api_fraction["legislature"]["id"]
            if api_fraction["legislature"]
            else None,
        }
        for api_fraction in api_fractions
    ]
    stmt = insert(Fraction).values(fractions)
    stmt = stmt.on_conflict_do_update(
        constraint="fraction_pkey",
        set_={col.name: col for col in stmt.excluded if not col.primary_key},
    )
    session = Session()
    session.execute(stmt)
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
    # TODO: parliament_period might only be a query parameter
    parliament_period_id = Column(Integer, ForeignKey("parliament_period.id"))
    parliament_period = relationship("ParliamentPeriod")
    electoral_data = relationship("ElectoralData", back_populates="constituency")


def populate_constituencies() -> None:
    api_constituencies = load_entity("constituencies")
    constituencies = [
        {
            "id": api_constituency["id"],
            "entity_type": api_constituency["entity_type"],
            "label": api_constituency["label"],
            "api_url": api_constituency["api_url"],
            "name": api_constituency["name"],
            "number": api_constituency["number"],
        }
        for api_constituency in api_constituencies
    ]
    session = Session()
    stmt = insert(Constituency).values(constituencies)
    stmt = stmt.on_conflict_do_update(
        constraint="constituency_pkey",
        set_={col.name: col for col in stmt.excluded if not col.primary_key},
    )
    session.execute(stmt)
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
    electoral_data = relationship("ElectoralData", back_populates="electoral_list")


def populate_electoral_lists() -> None:
    api_electoral_lists = load_entity("electoral-lists")
    electoral_lists = [
        {
            "id": api_electoral_list["id"],
            "entity_type": api_electoral_list["entity_type"],
            "label": api_electoral_list["label"],
            "api_url": api_electoral_list["api_url"],
            "name": api_electoral_list["name"],
            "parliament_period_id": api_electoral_list["parliament_period"]["id"]
            if api_electoral_list["parliament_period"]
            else None,
        }
        for api_electoral_list in api_electoral_lists
    ]
    session = Session()
    stmt = insert(ElectoralList).values(electoral_lists)
    stmt = stmt.on_conflict_do_update(
        constraint="electoral_list_pkey",
        set_={col.name: col for col in stmt.excluded if not col.primary_key},
    )
    session.execute(stmt)
    session.commit()
    session.close()


def link_length_checker_election_program():
    data_list = []
    data = load_entity("election-program")
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
    data = load_entity("election-program")
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


class ElectionProgram(Base):
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
    party = relationship("Party")


def populate_election_programs() -> None:
    api_election_programs = load_entity("election-program")
    election_programs = [
        {
            "id": api_election_program["id"],
            "entity_type": api_election_program["entity_type"],
            "label": api_election_program["label"],
            "api_url": api_election_program["api_url"],
            "parliament_period_id": api_election_program["parliament_period"]["id"]
            if api_election_program["parliament_period"]
            else None,
            "party_id": api_election_program["party"]["id"]
            if api_election_program["party"]
            else None,
            "link_uri": api_election_program["link"][0]["uri"],
            "link_title": api_election_program["link"][0]["title"],
            "link_option": api_election_program["link"][0]["option"]
            if api_election_program["link"][0].get("option")
            else None,
            "file": api_election_program["file"],
        }
        for api_election_program in api_election_programs
    ]
    stmt = insert(ElectionProgram).values(election_programs)
    stmt = stmt.on_conflict_do_update(
        constraint="election_program_pkey",
        set_={col.name: col for col in stmt.excluded if not col.primary_key},
    )
    session = Session()
    session.execute(stmt)
    session.commit()
    session.close()


class FractionMembership(Base):
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
        "CandidacyMandate", back_populates="fraction_membership"
    )


def populate_fraction_memberships() -> None:
    api_candidacies_mandates = load_entity("candidacies-mandates")
    fraction_memberships = []
    for api_candidacies_mandate in api_candidacies_mandates:
        fraction_membership = api_candidacies_mandate.get("fraction_membership")
        if fraction_membership:
            membership = fraction_membership[0]
            new_fraction_membership = {
                "id": membership["id"],
                "entity_type": membership["entity_type"],
                "label": membership["label"],
                "fraction_id": membership["fraction"]["id"],
                "valid_from": membership["valid_from"],
                "valid_until": membership["valid_until"],
            }
            fraction_memberships.append(new_fraction_membership)
    session = Session()
    stmt = insert(FractionMembership).values(fraction_memberships)
    stmt = stmt.on_conflict_do_update(
        constraint="fraction_membership_pkey",
        set_={col.name: col for col in stmt.excluded if not col.primary_key},
    )
    session.execute(stmt)
    session.commit()
    session.close()


class ElectoralData(Base):
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
        "CandidacyMandate", back_populates="electoral_data"
    )


def populate_electoral_data():
    api_candidacies_mandates = load_entity("candidacies-mandates")
    electoral_data_list = []
    for api_candidacies_mandate in api_candidacies_mandates:
        electoral_data = api_candidacies_mandate["electoral_data"]
        if electoral_data:
            new_electoral_data = {
                "id": electoral_data["id"],
                "entity_type": electoral_data["entity_type"],
                "label": electoral_data["label"],
                "electoral_list_id": electoral_data["electoral_list"]["id"]
                if electoral_data["electoral_list"]
                else None,
                "list_position": electoral_data["list_position"],
                "constituency_id": electoral_data["constituency"]["id"]
                if electoral_data["constituency"]
                else None,
                "constituency_result": electoral_data["constituency_result"],
                "constituency_result_count": electoral_data[
                    "constituency_result_count"
                ],
                "mandate_won": electoral_data["mandate_won"],
            }
            electoral_data_list.append(new_electoral_data)
    session = Session()
    stmt = insert(ElectoralData).values(electoral_data_list)
    stmt = stmt.on_conflict_do_update(
        constraint="electoral_data_pkey",
        set_={col.name: col for col in stmt.excluded if not col.primary_key},
    )
    session.execute(stmt)
    session.commit()
    session.close()


class CandidacyMandate(Base):
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
        "ElectoralData", back_populates="candidacy_mandate", uselist=False
    )
    fraction_membership = relationship(
        "FractionMembership", back_populates="candidacy_mandate", uselist=False
    )
    # One to Many
    committee_memberships = relationship(
        "CommitteeMembership", back_populates="candidacy_mandate"
    )
    votes = relationship("Vote", back_populates="candidacy_mandate")
    # Many to Many
    sidejobs = relationship(
        "Sidejob",
        secondary="sidejob_has_mandate",
        back_populates="candidacy_mandates",
    )


def populate_candidacies_mandates() -> None:
    api_candidacies_mandates = load_entity("candidacies-mandates")
    begin_time = time.time()
    candidacies_mandates = [
        {
            "id": api_candidacies_mandate["id"],
            "entity_type": api_candidacies_mandate["entity_type"],
            "label": api_candidacies_mandate["label"],
            "api_url": api_candidacies_mandate["api_url"],
            "id_external_administration": api_candidacies_mandate[
                "id_external_administration"
            ],
            "id_external_administration_description": api_candidacies_mandate[
                "id_external_administration_description"
            ],
            "type": api_candidacies_mandate["type"],
            "parliament_period_id": api_candidacies_mandate["parliament_period"]["id"]
            if api_candidacies_mandate["parliament_period"]
            else None,
            "politician_id": api_candidacies_mandate["politician"]["id"]
            if api_candidacies_mandate["politician"]
            else None,
            # Some dict don't include party itsself
            "party_id": api_candidacies_mandate["party"]["id"]
            if api_candidacies_mandate.get("party")
            else None,
            "start_date": api_candidacies_mandate["start_date"],
            "end_date": api_candidacies_mandate["end_date"],
            "info": api_candidacies_mandate["info"],
            "electoral_data_id": api_candidacies_mandate["electoral_data"]["id"]
            if api_candidacies_mandate["electoral_data"]
            else None,
            # Some dict don't include fraction_membership itsself
            "fraction_membership_id": api_candidacies_mandate["fraction_membership"][0][
                "id"
            ]
            if api_candidacies_mandate.get("fraction_membership")
            else None,
        }
        for api_candidacies_mandate in api_candidacies_mandates
    ]
    session = Session()
    stmt = insert(CandidacyMandate).values(candidacies_mandates)
    stmt = stmt.on_conflict_do_update(
        constraint="candidacy_mandate_pkey",
        set_={col.name: col for col in stmt.excluded if not col.primary_key},
    )
    session.execute(stmt)
    session.commit()
    session.close()
    end_time = time.time()
    print(
        f"Total runtime to store {len(candidacies_mandates)} data is {end_time - begin_time}"
    )


class CommitteeMembership(Base):
    __tablename__ = "committee_membership"
    id = Column(Integer, primary_key=True)
    entity_type = Column(String)
    label = Column(String)
    api_url = Column(String)
    committee_id = Column(Integer, ForeignKey("committee.id"))
    candidacy_mandate_id = Column(Integer, ForeignKey("candidacy_mandate.id"))
    committee_role = Column(String)
    # Many to One
    committee = relationship("Committee", back_populates="committee_memberships")
    candidacy_mandate = relationship(
        "CandidacyMandate", back_populates="committee_memberships"
    )


def populate_committee_memberships() -> None:
    api_committees = load_entity("committees")
    committee_ids = set([api_committee["id"] for api_committee in api_committees])
    api_committee_memberships = load_entity("committee-memberships")
    begin_time = time.time()
    committee_memberships = []
    for api_committee_membership in api_committee_memberships:
        committee_id = (
            api_committee_membership["committee"]["id"]
            if api_committee_membership["committee"]
            else None
        )
        if committee_id in committee_ids:
            new_membership = {
                "id": api_committee_membership["id"],
                "entity_type": api_committee_membership["entity_type"],
                "label": api_committee_membership["label"],
                "api_url": api_committee_membership["api_url"],
                "committee_id": api_committee_membership["committee"]["id"]
                if api_committee_membership["committee"]
                else None,
                "candidacy_mandate_id": api_committee_membership["candidacy_mandate"][
                    "id"
                ]
                if api_committee_membership["candidacy_mandate"]
                else None,
                "committee_role": api_committee_membership["committee_role"],
            }
            committee_memberships.append(new_membership)
    session = Session()
    stmt = insert(CommitteeMembership).values(committee_memberships)
    stmt = stmt.on_conflict_do_update(
        constraint="committee_membership_pkey",
        set_={col.name: col for col in stmt.excluded if not col.primary_key},
    )
    session.execute(stmt)
    session.commit()
    session.close()
    end_time = time.time()
    print(
        f"Total runtime to store {len(committee_memberships)} data is {end_time - begin_time}"
    )


class Poll(Base):
    __tablename__ = "poll"
    id = Column(Integer, primary_key=True)
    entity_type = Column(String)
    label = Column(String)
    api_url = Column(String)
    field_committees_id = Column(Integer, ForeignKey("committee.id"))
    field_intro = Column(Text)
    field_legislature_id = Column(Integer, ForeignKey("parliament_period.id"))
    field_poll_date = Column(Date)
    # Many to One
    committee = relationship("Committee", back_populates="polls")
    parliament_period = relationship("ParliamentPeriod", back_populates="polls")
    # Many to Many
    topics = relationship("Topic", secondary="poll_has_topic", back_populates="polls")
    # One to Many
    field_related_links = relationship("FieldRelatedLink", back_populates="poll")
    votes = relationship("Vote", back_populates="poll")


def populate_polls() -> None:
    api_polls = load_entity("polls")
    polls = [
        {
            "id": api_polls["id"],
            "entity_type": api_polls["entity_type"],
            "label": api_polls["label"],
            "api_url": api_polls["api_url"],
            "field_committees_id": api_polls["field_committees"][0]["id"]
            if api_polls["field_committees"]
            else None,
            "field_intro": api_polls["field_intro"],
            "field_legislature_id": api_polls["field_legislature"]["id"]
            if api_polls["field_legislature"]
            else None,
            "field_poll_date": api_polls["field_poll_date"],
        }
        for api_polls in api_polls
    ]
    session = Session()
    stmt = insert(Poll).values(polls)
    stmt = stmt.on_conflict_do_update(
        constraint="poll_pkey",
        set_={col.name: col for col in stmt.excluded if not col.primary_key},
    )
    session.execute(stmt)
    session.commit()
    session.close()


class PollHasTopic(Base):
    __tablename__ = "poll_has_topic"
    poll_id = Column(Integer, ForeignKey("poll.id"), primary_key=True)
    topic_id = Column(Integer, ForeignKey("topic.id"), primary_key=True)


def populate_poll_has_topic() -> None:
    api_polls = load_entity("polls")
    polls_topics = []
    for api_poll in api_polls:
        field_topics = api_poll["field_topics"]
        if field_topics:
            for topic in field_topics:
                poll_topic = {
                    "poll_id": api_poll["id"],
                    "topic_id": topic["id"],
                }
                polls_topics.append(poll_topic)
    session = Session()
    stmt = insert(PollHasTopic).values(polls_topics)
    stmt = stmt.on_conflict_do_nothing()
    session.execute(stmt)
    session.commit()
    session.close()


class FieldRelatedLink(Base):
    __tablename__ = "field_related_link"
    id = Column(Integer, primary_key=True, autoincrement=True)
    poll_id = Column(Integer, ForeignKey("poll.id"))
    uri = Column(String)
    title = Column(String)
    # Many to One
    poll = relationship("Poll", back_populates="field_related_links")


def populate_field_related_link() -> None:
    api_polls = load_entity("polls")
    poll_related_links = []
    for api_poll in api_polls:
        poll_id = api_poll["id"]
        field_related_links = api_poll["field_related_links"]
        if field_related_links:
            for field_related_link in field_related_links:
                poll_related_link = {
                    "poll_id": poll_id,
                    "uri": field_related_link["uri"],
                    "title": field_related_link["title"],
                }
                poll_related_links.append(poll_related_link)
    session = Session()
    stmt = insert(FieldRelatedLink).values(poll_related_links)
    stmt = stmt.on_conflict_do_update(
        constraint="field_related_link_pkey",
        set_={col.name: col for col in stmt.excluded if not col.primary_key},
    )
    session.execute(stmt)
    session.commit()
    session.close()


class Vote(Base):
    __tablename__ = "vote"
    id = Column(Integer, primary_key=True)
    entity_type = Column(String)
    label = Column(String)
    api_url = Column(String)
    mandate_id = Column(Integer, ForeignKey("candidacy_mandate.id"))
    fraction_id = Column(Integer, ForeignKey("fraction.id"))
    poll_id = Column(Integer, ForeignKey("poll.id"))
    vote = Column(String)
    reason_no_show = Column(String)
    reason_no_show_other = Column(String)
    # Many to One
    candidacy_mandate = relationship("CandidacyMandate", back_populates="votes")
    fraction = relationship("Fraction", back_populates="votes")
    poll = relationship("Poll", back_populates="votes")


def populate_votes():
    api_polls = load_entity("polls")
    poll_ids = set([api_poll["id"] for api_poll in api_polls])
    api_votes = load_entity("votes")
    begin_time = time.time()
    votes = []
    for api_vote in api_votes:
        poll_id = api_vote["poll"]["id"] if api_vote["poll"] else None
        if poll_id in poll_ids:
            vote = {
                "id": api_vote["id"],
                "entity_type": api_vote["entity_type"],
                "label": api_vote["label"],
                "api_url": api_vote["api_url"],
                "mandate_id": api_vote["mandate"]["id"]
                if api_vote["mandate"]
                else None,
                "fraction_id": api_vote["fraction"]["id"]
                if api_vote["fraction"]
                else None,
                "poll_id": poll_id,
                "vote": api_vote["vote"],
                "reason_no_show": api_vote["reason_no_show"],
                "reason_no_show_other": api_vote["reason_no_show_other"],
            }
            votes.append(vote)
    session = Session()
    stmt = insert(Vote).values(votes)
    stmt = stmt.on_conflict_do_update(
        constraint="vote_pkey",
        set_={col.name: col for col in stmt.excluded if not col.primary_key},
    )
    session.execute(stmt)
    session.commit()
    session.close()
    end_time = time.time()
    print(f"Total runtime to store {len(api_votes)} data is {end_time - begin_time}")


class SidejobOrganization(Base):
    __tablename__ = "sidejob_organization"
    id = Column(Integer, primary_key=True)
    entity_type = Column(String)
    label = Column(String)
    api_url = Column(String)
    field_city_id = Column(Integer, ForeignKey("city.id"))
    field_country_id = Column(Integer, ForeignKey("country.id"))
    # Many to One
    city = relationship("City", back_populates="sidejob_organizations")
    country = relationship("Country", back_populates="sidejob_organizations")
    # One to Many
    sidejobs = relationship("Sidejob", back_populates="sidejob_organization")
    # Many to Many
    topics = relationship(
        "Topic",
        secondary="sidejob_organization_has_topic",
        back_populates="sidejob_organizations",
    )


def populate_sidejob_organizations() -> None:
    api_sidejob_organizations = load_entity("sidejob-organizations")
    sidejob_organizations = [
        {
            "id": api_sidejob_organization["id"],
            "entity_type": api_sidejob_organization["entity_type"],
            "label": api_sidejob_organization["label"],
            "api_url": api_sidejob_organization["api_url"],
            "field_city_id": api_sidejob_organization["field_city"]["id"]
            if api_sidejob_organization["field_city"]
            else None,
            "field_country_id": api_sidejob_organization["field_country"]["id"]
            if api_sidejob_organization["field_country"]
            else None,
        }
        for api_sidejob_organization in api_sidejob_organizations
    ]
    session = Session()
    stmt = insert(SidejobOrganization).values(sidejob_organizations)
    stmt = stmt.on_conflict_do_update(
        constraint="sidejob_organization_pkey",
        set_={col.name: col for col in stmt.excluded if not col.primary_key},
    )
    session.execute(stmt)
    session.commit()
    session.close()


class SidejobOrganizationHasTopic(Base):
    __tablename__ = "sidejob_organization_has_topic"
    sidejob_organization_id = Column(
        Integer, ForeignKey("sidejob_organization.id"), primary_key=True
    )
    topic_id = Column(Integer, ForeignKey("topic.id"), primary_key=True)


def populate_sidejob_organization_has_topic() -> None:
    api_sidejob_organizations = load_entity("sidejob-organizations")
    organization_topics = []
    for api_sidejob_organization in api_sidejob_organizations:
        field_topics = api_sidejob_organization["field_topics"]
        if field_topics:
            for topic in field_topics:
                organization_topic = {
                    "sidejob_organization_id": api_sidejob_organization["id"],
                    "topic_id": topic["id"],
                }
                organization_topics.append(organization_topic)
    session = Session()
    stmt = insert(SidejobOrganizationHasTopic).values(organization_topics)
    stmt = stmt.on_conflict_do_nothing()
    session.execute(stmt)
    session.commit()
    session.close()


class Sidejob(Base):
    __tablename__ = "sidejob"
    id = Column(Integer, primary_key=True)
    entity_type = Column(String)
    label = Column(String)
    api_url = Column(String)
    job_title_extra = Column(String)
    additional_information = Column(String)
    category = Column(String)
    income_level = Column(String)
    interval = Column(String)
    data_change_date = Column(Date)
    created = Column(Integer)
    sidejob_organization_id = Column(Integer, ForeignKey("sidejob_organization.id"))
    field_city_id = Column(Integer, ForeignKey("city.id"))
    field_country_id = Column(Integer, ForeignKey("country.id"))
    # Many to One
    sidejob_organization = relationship(
        "SidejobOrganization", back_populates="sidejobs"
    )
    city = relationship("City", back_populates="sidejobs")
    country = relationship("Country", back_populates="sidejobs")
    # Many to Many
    candidacy_mandates = relationship(
        "CandidacyMandate",
        secondary="sidejob_has_mandate",
        back_populates="sidejobs",
    )
    topics = relationship(
        "Topic",
        secondary="sidejob_has_topic",
        back_populates="sidejobs",
    )


def populate_sidejobs() -> None:
    api_sidejobs = load_entity("sidejobs")
    sidejobs = [
        {
            "id": api_sidejob["id"],
            "entity_type": api_sidejob["entity_type"],
            "label": api_sidejob["label"],
            "api_url": api_sidejob["api_url"],
            "job_title_extra": api_sidejob["job_title_extra"],
            "additional_information": api_sidejob["additional_information"],
            "category": api_sidejob["category"],
            "income_level": api_sidejob["income_level"],
            "interval": api_sidejob["interval"],
            "data_change_date": api_sidejob["data_change_date"],
            "created": api_sidejob["created"],
            "sidejob_organization_id": api_sidejob["sidejob_organization"]["id"]
            if api_sidejob["sidejob_organization"]
            else None,
            "field_city_id": api_sidejob["field_city"]["id"]
            if api_sidejob["field_city"]
            else None,
            "field_country_id": api_sidejob["field_country"]["id"]
            if api_sidejob["field_country"]
            else None,
        }
        for api_sidejob in api_sidejobs
    ]
    session = Session()
    stmt = insert(Sidejob).values(sidejobs)
    stmt = stmt.on_conflict_do_update(
        constraint="sidejob_pkey",
        set_={col.name: col for col in stmt.excluded if not col.primary_key},
    )
    session.execute(stmt)
    session.commit()
    session.close()


class SidejobHasMandate(Base):
    __tablename__ = "sidejob_has_mandate"
    sidejob_id = Column(Integer, ForeignKey("sidejob.id"), primary_key=True)
    candidacy_mandate_id = Column(
        Integer, ForeignKey("candidacy_mandate.id"), primary_key=True
    )


def generate_unique_sidejob_mandate():
    data_list = []
    data = json_fetch("sidejob")
    for datum in data:
        if datum["mandates"]:
            for mandate in datum["mandates"]:
                new_data = {
                    "sidejob_id": datum["id"],
                    "candidacy_mandate_id": mandate["id"],
                }
                data_list.append(new_data)
    return [dict(t) for t in {tuple(d.items()) for d in data_list}]


def populate_sidejob_has_mandate():
    data_list = []
    data = generate_unique_sidejob_mandate()
    for datum in data:
        new_datum = SidejobHasMandate(
            sidejob_id=datum["sidejob_id"],
            candidacy_mandate_id=datum["candidacy_mandate_id"],
        )
        data_list.append(new_datum)

    session.add_all(data_list)
    session.commit()
    session.close()


class SidejobHasTopic(Base):
    __tablename__ = "sidejob_has_topic"
    sidejob_id = Column(Integer, ForeignKey("sidejob.id"), primary_key=True)
    topic_id = Column(Integer, ForeignKey("topic.id"), primary_key=True)


def generate_unique_sidejob_topic_ids():
    data_list = []
    data = json_fetch("sidejob")
    for datum in data:
        if datum["field_topics"]:
            for topic in datum["field_topics"]:
                new_data = {
                    "sidejob_id": datum["id"],
                    "topic_id": topic["id"],
                }
                data_list.append(new_data)
    return [dict(t) for t in {tuple(d.items()) for d in data_list}]


def populate_sidejob_has_topic():
    data_list = []
    data = generate_unique_sidejob_topic_ids()
    for datum in data:
        new_datum = SidejobHasTopic(
            sidejob_id=datum["sidejob_id"], topic_id=datum["topic_id"]
        )
        data_list.append(new_datum)

    session.add_all(data_list)
    session.commit()
    session.close()


class Position_statement(Base):
    __tablename__ = "position_statement"
    # id has the following structure parliament_period + statement_number (130 + 1 -> 1301)
    id = Column(Integer(), primary_key=True)
    statement = Column(String)
    topic_id = Column(Integer, ForeignKey("topic.id"))
    topics = relationship("Topic", back_populates="position_statements")
    positions = relationship("Position", back_populates="position_statements")

    def insert_position_statement():
        data_list = []
        # parliament period needs to match the assumptions of the state
        parliament_period = "130"
        # add json file with the assumptions to the src directory
        file = "src/mecklenburg-vorpommern-assumptions.json"
        with open(file) as f:
            data = json.load(f)
            for assumption in data:
                i = parliament_period + str(assumption["number"])
                newData = Position_statement(
                    id=int(i),
                    statement=assumption["text"],
                    topic_id=assumption["topic"],
                )
                data_list.append(newData)
            session.add_all(data_list)
            session.commit()
            session.close()


class Position(Base):
    __tablename__ = "position"
    # id has the following structure parliament_period + statement_number (130 + 1 -> 1301)
    id = Column(BigInteger, primary_key=True)
    position = Column(String)
    reason = Column(String())
    politician_id = Column(Integer, ForeignKey("politician.id"))
    parliament_period_id = Column(Integer, ForeignKey("parliament_period.id"))
    position_statement_id = Column(Integer, ForeignKey("position_statement.id"))
    politicians = relationship("Politician", back_populates="positions")
    parliament_periods = relationship("ParliamentPeriod", back_populates="positions")
    position_statements = relationship("Position_statement", back_populates="positions")

    def insert_position():
        data_list = []
        # parliament period needs to match the assumptions of the state
        parliament_period = "130"
        # add json file with the assumptions to the src directory
        file = "src/mecklenburg-vorpommern-positions.json"
        with open(file) as f:
            data = json.load(f)
            for politician in data:
                for position_data in data[politician]:
                    pk_id = (
                        parliament_period
                        + str(politician)
                        + str(list(position_data.keys())[0])
                    )
                    fk_id = parliament_period + str(list(position_data.keys())[0])
                    newData = Position(
                        id=int(pk_id),
                        position=position_data[list(position_data.keys())[0]][
                            "position"
                        ],
                        reason=position_data[list(position_data.keys())[0]]["reason"]
                        if "reason" in position_data[list(position_data.keys())[0]]
                        else None,
                        politician_id=int(politician),
                        parliament_period_id=int(parliament_period),
                        position_statement_id=int(fk_id),
                    )
                    data_list.append(newData)
            session.add_all(data_list)
            session.commit()
            session.close()


class CV(Base):
    __tablename__ = "cv"
    id = Column(Integer, primary_key=True, autoincrement=True)
    politician_id = Column(Integer, ForeignKey("politician.id"))
    raw_text = Column(String)
    short_description = Column(String)
    # Many to One
    politician = relationship("Politician", back_populates="cvs")
    # One to Many
    career_paths = relationship("CareerPath", back_populates="cv")


def populate_cv():
    data_list = []
    politician_ids = cv_json_file_numbers_generator()

    for politician_id in politician_ids:
        json_file = cv_json_fetch("{}".format(politician_id))
        bio = json_file["Biography"]
        new_datum = CV(
            politician_id=politician_id,
            raw_text=bio["Raw"],
            short_description=bio["ShortDescription"],
        )
        data_list.append(new_datum)

    session.add_all(data_list)
    session.commit()
    session.close()


class CareerPath(Base):
    __tablename__ = "career_path"
    id = Column(Integer, primary_key=True, autoincrement=True)
    cv_id = Column(Integer, ForeignKey("cv.id"))
    raw_text = Column(String)
    label = Column(String)
    period = Column(String)
    # Many to One
    cv = relationship("CV", back_populates="career_paths")


def populate_career_path():
    data_list = []
    politician_ids = cv_json_file_numbers_generator()
    for politician_id in politician_ids:
        json_file = cv_json_fetch("{}".format(politician_id))
        steps = json_file["Biography"]["Steps"]
        if steps != None:
            row = session.query(CV).filter(CV.politician_id == politician_id).first()
            cv_id = row.id
            for step in steps:
                new_datum = CareerPath(
                    cv_id=cv_id,
                    raw_text=step["Raw"],
                    label=step["Label"],
                    period=step["Date"],
                )
                data_list.append(new_datum)
    session.add_all(data_list)
    session.commit()
    session.close()


if __name__ == "__main__":
    # Migration =>Table creation
    Base.metadata.create_all(engine)
    # populate_countries()
    # populate_cities()
    # populate_parties()
    # populate_politicians()
    # populate_parliaments()
    # populate_parliament_periods()
    # populate_topics()
    # populate_committees()
    # populate_committee_has_topic()
    # populate_fractions()
    # populate_constituencies()
    # populate_electoral_lists()
    # populate_election_programs()
    # populate_fraction_memberships()
    # populate_electoral_data()
    # populate_candidacies_mandates()
    # populate_committee_memberships()
    # populate_polls()
    # populate_poll_has_topic()
    # populate_field_related_link()
    # populate_votes()
    # populate_sidejob_organizations()
    # populate_sidejob_organization_has_topic()
    populate_sidejobs()

    # populate_vote()
    # PositionStatement.insert_position_statement()
    # Position.insert_position()
    # Position_statement.insert_position_statement()
    # Committee.insert_committee(committee_fetch())
