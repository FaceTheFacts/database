from typing import Any, TypedDict
import requests
import time
import math


PAGE_SIZE = 999


class ApiResponse(TypedDict):
    meta: dict[str, Any]
    data: list[Any]


def request(url: str) -> ApiResponse:
    try:
        response = requests.get(url)
    except requests.exceptions.RequestException as err:
        raise Exception(err)
    return response.json()


def fetch_page(entity: str, page_nr: int) -> list[Any]:
    url = f"https://www.abgeordnetenwatch.de/api/v2/{entity}?range_start={page_nr * PAGE_SIZE}&range_end={PAGE_SIZE}"
    result: ApiResponse = request(url)
    return result["data"]


def fetch_entity(entity: str) -> list[Any]:
    time_begin = time.time()
    url = f"https://www.abgeordnetenwatch.de/api/v2/{entity}?range_end=0"
    result = request(url)
    total = result["meta"]["result"]["total"]
    page_count = math.ceil(total / PAGE_SIZE)
    entities = [None] * total
    for page_nr in range(page_count):
        page_entities = fetch_page(entity, page_nr)
        print(f"Page No.{page_nr} of {entity} is fetched")
        for i in range(len(page_entities)):
            entities[i + page_nr * PAGE_SIZE] = page_entities[i]

    print("All data is fetched!")
    time_end = time.time()
    print(f"Total runtime of fetching {entity} is {time_end - time_begin}")

    return entities


def fetch(entity: str):
    begin = time.time()
    BASE_URL = "https://www.abgeordnetenwatch.de/api/v2/{entity}?&page={page}&pager_limit={pager_limit}"
    page_number = 0
    pager_limit = 1000
    finished = False
    fetched_data_list = []
    while not finished:
        url = BASE_URL.format(entity=entity, page=page_number, pager_limit=pager_limit)
        response = requests.get(url)
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as error:
            return error
        data = response.json()["data"]

        if not data:
            finished = True

        fetched_data_list += data
        print("Page No.{page} is fetched".format(page=page_number))
        page_number += 1
    print("All data is fetched!")
    end = time.time()
    print(f"Total runtime of fetching is {end - begin}")
    return fetched_data_list


# Categories without foreign keys
def party_fetch():
    return fetch("parties")


def city_fetch():
    return fetch("cities")


def country_fetch():
    return fetch("countries")


def topic_fetch():
    return fetch("topics")


# Categories with Foreign Keys
def politician_fetch():
    return fetch("politicians")


def committee_fetch():
    return fetch("committees")


def parliament_fetch():
    return fetch("parliaments")


def parliament_period_fetch():
    return fetch("parliament-periods")


def fraction_fetch():
    return fetch("fractions")


def constituency_fetch():
    return fetch("constituencies")


def electoral_list_fetch():
    return fetch("electoral-lists")


def election_program_fetch():
    return fetch("election-program")


def candidacy_mandate_fetch():
    return fetch("candidacies-mandates")


def committee_memberships_fetch():
    return fetch("committee-memberships")
