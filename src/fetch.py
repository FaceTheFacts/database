import requests


def fetch(entity: str):
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
    return fetched_data_list


# Categories without foreign keys
def party_fetch():
    return fetch("parties")


def city_fetch():
    return fetch("cities")


def country_fetch():
    return fetch("countries")


# Categories with Foreign Keys
def politician_fetch():
    return fetch("politicians")


def parliament_fetch():
    return fetch("parliaments")


def parliament_periods_fetch():
    return fetch("parliament-periods")
