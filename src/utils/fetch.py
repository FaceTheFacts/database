from typing import Any, TypedDict
import requests
import time
import math

from .file import read_json, write_json, has_valid_file


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


def load_entity(entity: str) -> list[Any]:
    file_path = f"src/data/{entity}.json"
    has_file = has_valid_file(file_path)
    if not has_file:
        data = fetch_entity(entity)
        write_json(file_path, data)
        return data

    data: list[Any] = read_json(file_path)
    return data
