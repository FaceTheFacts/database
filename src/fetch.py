import requests


def fetch(url: str):
    BASE_URL = "https://www.abgeordnetenwatch.de/api/v2"
    response = requests.get(f"{BASE_URL}/{url}")
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as error:
        return error
    data = response.json()["data"]
    return data


def party_fetch():
    return fetch("parties")
