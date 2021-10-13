import time
from sqlalchemy.dialects.postgresql import insert

from ..utils.fetch import load_entity
from ..db.session import engine, Session



from ..models.country import Country
from ..models.city import City
from ..models.party import Party
from ..models.politician import Politician
from ..models.parliament import Parliament
from ..models.parliament_period import ParliamentPeriod
from ..models.topic import Topic
from ..models.committee import Committee
from ..models.committee_has_topic import CommitteeHasTopic
from ..models.fraction import Fraction
from ..models.constituency import Constituency
from ..models.electoral_list import ElectoralList
from ..models.election_program import ElectionProgram
from ..models.fraction_membership import FractionMembership
from ..models.electoral_data import ElectoralData
from ..models.candidacy_mandate import CandidacyMandate
from ..models.committee_membership import CommitteeMembership
from ..models.poll import Poll
from ..models.poll_has_topic import PollHasTopic
from ..models.field_related_link import FieldRelatedLink
from ..models.vote import Vote
from ..models.sidejob_organization import SidejobOrganization
from ..models.sidejob_organization_has_topic import SidejobOrganizationHasTopic
from ..models.sidejob import Sidejob
from ..models.sidejob_has_mandate import SidejobHasMandate
from ..models.sidejob_has_topic import SidejobHasTopic


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


def update_parliament_current_project_ids() -> None:
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


def populate_parliament_periods() -> None:
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


def populate_topics() -> None:
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


def populate_committees() -> None:
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


# def link_length_checker_election_program():
#     data_list = []
#     data = load_entity("election-program")
#     length_of_data = len(data)
#     for datum in data:
#         link = datum["link"]
#         length_of_link = len(link)
#         if length_of_link != 1:
#             data_list.append(datum)

#     print("Fetched {} data in total".format(length_of_data))
#     print("{} has multiple links".format(data_list))


# def link_checker_election_program():
#     data_list = []
#     data = load_entity("election-program")
#     length_of_data = len(data)
#     for datum in data:
#         id = datum["id"]
#         link = datum["link"]
#         uri = link[0]["uri"]
#         if uri != None:
#             hasUrl = {"id": id, "uri": uri}
#             data_list.append(hasUrl)
#     print("Fetched {} data in total".format(length_of_data))
#     print("{} in total have uris".format(len(data_list)))


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


def populate_electoral_data() -> None:
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


def populate_votes() -> None:
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


def populate_sidejob_has_mandate() -> None:
    api_sidejobs = load_entity("sidejobs")
    sidejob_mandates = []
    for api_sidejob in api_sidejobs:
        mandates = api_sidejob["mandates"]
        if mandates:
            for mandate in mandates:
                sidejob_mandate = {
                    "sidejob_id": api_sidejob["id"],
                    "candidacy_mandate_id": mandate["id"],
                }
                sidejob_mandates.append(sidejob_mandate)
    stmt = insert(SidejobHasMandate).values(sidejob_mandates)
    stmt = stmt.on_conflict_do_nothing()
    session = Session()
    session.execute(stmt)
    session.commit()
    session.close()


def populate_sidejob_has_topic() ->None:
    api_sidejobs = load_entity("sidejobs")
    sidejob_topics = []
    for api_sidejob in api_sidejobs:
        field_topics = api_sidejob["field_topics"]
        if field_topics:
            for topic in field_topics:
                sidejob_topic = {
                    "sidejob_id": api_sidejob["id"],
                    "topic_id": topic["id"],
                }
                sidejob_topics.append(sidejob_topic)
    stmt = insert(SidejobHasTopic).values(sidejob_topics)
    stmt = stmt.on_conflict_do_nothing()
    session = Session()
    session.execute(stmt)
    session.commit()
    session.close()


if __name__ == "__main__":
    # populate_countries()
    # populate_cities()
    # populate_parties()
    # populate_politicians()
    # populate_parliaments()
    # update_parliament_current_project_ids()
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
    # populate_sidejobs()
    # populate_sidejob_has_mandate()
    populate_sidejob_has_topic()
