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
    election_program_fetch,
    electoral_list_fetch,
    fraction_fetch,
    topic_fetch,
)
import json
from fetch import load_entity

import sys

sys.path.append("src")
from data.json_handler import json_fetch, json_generator

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
    candidacy_mandates = relationship("Candidacy_mandate", back_populates="party")


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
    candidacy_mandates = relationship("Candidacy_mandate", back_populates="politician")
    positions = relationship("Position", back_populates="politicians")


def populate_politicians() -> None:
    begin_time = time.time()
    api_politicians = load_entity("politicians")
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
        "Candidacy_mandate", back_populates="parliament_period"
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
        "Committee_membership", back_populates="committee"
    )
    polls = relationship("Poll", back_populates="committee")

    def insert_committee(data: list):
        data_list = []
        for datum in data:
            new_data = Committee(
                id=datum["id"],
                entity_type=datum["entity_type"],
                label=datum["label"],
                api_url=datum["api_url"],
                field_legislature_id=datum["field_legislature"]["id"],
            )
            data_list.append(new_data)
        session.add_all(data_list)
        session.commit()
        session.close()


class Committee_has_topic(Base):
    __tablename__ = "committee_has_topic"
    committee_id = Column(Integer(), ForeignKey("committee.id"), primary_key=True)
    topic_id = Column(Integer(), ForeignKey("topic.id"), primary_key=True)

    def insert_committee_has_topic(data: list):
        data_list = []
        for datum in data:
            if datum["field_topics"]:
                for topic in datum["field_topics"]:
                    new_data = Committee_has_topic(
                        committee_id=datum["id"], topic_id=topic["id"]
                    )
                    data_list.append(new_data)
        session.add_all(data_list)
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
    # One to Many
    votes = relationship("Vote", back_populates="fraction")


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
    parliament_period = relationship("ParliamentPeriod")
    electoral_data = relationship("Electoral_data", back_populates="constituency")


def isParliamentPeriod():
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
    parliament_period = relationship("ParliamentPeriod")
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
    # One to Many
    committee_memberships = relationship(
        "Committee_membership", back_populates="candidacy_mandate"
    )
    votes = relationship("Vote", back_populates="candidacy_mandate")
    # Many to Many
    sidejobs = relationship(
        "Sidejob",
        secondary="sidejob_has_mandate",
        back_populates="candidacy_mandates",
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


class Committee_membership(Base):
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
        "Candidacy_mandate", back_populates="committee_memberships"
    )


def is_exist_committee(id: int) -> Boolean:
    command = select(Committee.id).where(Committee.id == id)
    result = session.execute(command).fetchone()
    if result != None:
        return True
    else:
        return False


def insert_committee_membership():
    begin = time.time()
    committee_membership = []
    non_committee_data = []
    data = json_fetch("committee_membership_ids")
    for datum in data:
        is_exist_in_committee = is_exist_committee(datum["committee_id"])
        if is_exist_in_committee:
            new_datum = Committee_membership(
                id=datum["id"],
                entity_type=datum["entity_type"],
                label=datum["label"],
                api_url=datum["api_url"],
                committee_id=datum["committee_id"],
                candidacy_mandate_id=datum["candidacy_mandate_id"],
                committee_role=datum["committee_role"],
            )
            committee_membership.append(new_datum)
        else:
            non_committee_data.append(datum)
    session.add_all(committee_membership)
    session.commit()
    print("Inserted {} data in total".format(len(committee_membership)))
    json_generator(non_committee_data, "non_committee_committee_membership")
    end = time.time()
    print(f"Total runtime of insert is {end - begin}")
    session.close()


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


def populate_poll():
    data_list = []
    data = json_fetch("poll")
    for datum in data:
        new_datum = Poll(
            id=datum["id"],
            entity_type=datum["entity_type"],
            label=datum["label"],
            api_url=datum["api_url"],
            field_committees_id=datum["field_committees"][0]["id"]
            if datum["field_committees"]
            else None,
            field_intro=datum["field_intro"],
            field_legislature_id=datum["field_legislature"]["id"]
            if datum["field_legislature"]
            else None,
            field_poll_date=datum["field_poll_date"],
        )
        data_list.append(new_datum)
    session.add_all(data_list)
    session.commit()
    print("Inserted {} data in total".format(len(data_list)))
    session.close()


class PollHasTopic(Base):
    __tablename__ = "poll_has_topic"
    poll_id = Column(Integer, ForeignKey("poll.id"), primary_key=True)
    topic_id = Column(Integer, ForeignKey("topic.id"), primary_key=True)


def populate_poll_has_topic():
    data_list = []
    data = json_fetch("poll")
    for datum in data:
        if datum["field_topics"]:
            for topic in datum["field_topics"]:
                new_data = PollHasTopic(
                    poll_id=datum["id"],
                    topic_id=topic["id"],
                )

                data_list.append(new_data)
    session.add_all(data_list)
    session.commit()
    print("Inserted {} data in total".format(len(data_list)))
    session.close()


class FieldRelatedLink(Base):
    __tablename__ = "field_related_link"
    id = Column(Integer, primary_key=True, autoincrement=True)
    poll_id = Column(Integer, ForeignKey("poll.id"))
    uri = Column(String)
    title = Column(String)
    # Many to One
    poll = relationship("Poll", back_populates="field_related_links")


def populate_field_related_link():
    data_list = []
    data = json_fetch("poll")
    for datum in data:
        id = datum["id"]
        field_related_links = datum.get("field_related_links")
        if field_related_links != None:
            for field_related_link in field_related_links:
                new_datum = FieldRelatedLink(
                    poll_id=id,
                    uri=field_related_link["uri"],
                    title=field_related_link["title"],
                )
                data_list.append(new_datum)
    session.add_all(data_list)
    session.commit()
    print("Inserted {} data in total".format(len(data_list)))
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
    candidacy_mandate = relationship("Candidacy_mandate", back_populates="votes")
    fraction = relationship("Fraction", back_populates="votes")
    poll = relationship("Poll", back_populates="votes")


def poll_ids_list_generator():
    data_list = []
    data = json_fetch("poll")
    for datum in data:
        data_list.append(datum["id"])
    return list(set(data_list))


def populate_vote():
    begin = time.time()
    data_list = []
    missing_list = []
    data = json_fetch("vote")
    poll_id_list = poll_ids_list_generator()
    for datum in data:
        poll_id = datum["poll"]["id"] if datum["poll"] else None
        is_exist_poll_id = poll_id in poll_id_list
        if is_exist_poll_id:
            new_datum = Vote(
                id=datum["id"],
                entity_type=datum["entity_type"],
                label=datum["label"],
                api_url=datum["api_url"],
                mandate_id=datum["mandate"]["id"] if datum["mandate"] else None,
                fraction_id=datum["fraction"]["id"] if datum["fraction"] else None,
                poll_id=poll_id,
                vote=datum["vote"],
                reason_no_show=datum["reason_no_show"],
                reason_no_show_other=datum["reason_no_show_other"],
            )
            data_list.append(new_datum)
        else:
            missing_list.append(datum)
    json_generator(missing_list, "no_poll_id_vote")
    session.add_all(data_list)
    session.commit()
    print("Inserted {} data in total".format(len(data_list)))
    session.close()
    end = time.time()
    print(f"Total runtime to store {len(data_list)} data is {end - begin}")


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


def populate_sidejob_organization():
    data_list = []
    data = json_fetch("sidejob_organization_ids")
    for datum in data:
        new_datum = SidejobOrganization(
            id=datum["id"],
            entity_type=datum["entity_type"],
            label=datum["label"],
            api_url=datum["api_url"],
            field_city_id=datum["field_city_id"],
            field_country_id=datum["field_country_id"],
        )
        data_list.append(new_datum)
    session.add_all(data_list)
    session.commit()
    print("Inserted {} data in total".format(len(data_list)))
    session.close()


class SidejobOrganizationHasTopic(Base):
    __tablename__ = "sidejob_organization_has_topic"
    sidejob_organization_id = Column(
        Integer, ForeignKey("sidejob_organization.id"), primary_key=True
    )
    topic_id = Column(Integer, ForeignKey("topic.id"), primary_key=True)


def populate_sidejob_has_topic():
    data_list = []
    data = json_fetch("sidejob_organization")
    for datum in data:
        if datum["field_topics"]:
            for topic in datum["field_topics"]:
                new_data = SidejobOrganizationHasTopic(
                    sidejob_organization_id=datum["id"], topic_id=topic["id"]
                )
                data_list.append(new_data)
    session.add_all(data_list)
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
        "Candidacy_mandate",
        secondary="sidejob_has_mandate",
        back_populates="sidejobs",
    )
    topics = relationship(
        "Topic",
        secondary="sidejob_has_topic",
        back_populates="sidejobs",
    )


def populate_sidejob():
    data_list = []
    data = json_fetch("sidejob")
    for datum in data:
        new_datum = Sidejob(
            id=datum["id"],
            entity_type=datum["entity_type"],
            label=datum["label"],
            api_url=datum["api_url"],
            job_title_extra=datum["job_title_extra"],
            additional_information=datum["additional_information"],
            category=datum["category"],
            income_level=datum["income_level"],
            interval=datum["interval"],
            data_change_date=datum["data_change_date"],
            created=datum["created"],
            sidejob_organization_id=datum["sidejob_organization"]["id"]
            if datum["sidejob_organization"]
            else None,
            field_city_id=datum["field_city"]["id"] if datum["field_city"] else None,
            field_country_id=datum["field_country"]["id"]
            if datum["field_country"]
            else None,
        )
        data_list.append(new_datum)
    session.add_all(data_list)
    session.commit()
    print("Inserted {} data in total".format(len(data_list)))
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


if __name__ == "__main__":
    # Migration =>Table creation
    Base.metadata.create_all(engine)
    # populate_vote()
    # PositionStatement.insert_position_statement()
    # Position.insert_position()
    # Position_statement.insert_position_statement()
    # Committee.insert_committee(committee_fetch())
    # Committee_has_topic.insert_committee_has_topic(committee_fetch())
    # populate_countries()
    # populate_cities()
    # populate_parties()
    # populate_politicians()
    # populate_parliaments()
    # populate_parliament_periods()
    populate_topics()
