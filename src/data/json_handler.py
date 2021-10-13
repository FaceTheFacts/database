import json
import sys
import os

sys.path.append("src")
from fetch import fetch


def json_generator(data: list, file_name: str) -> None:
    fetched_data = data

    with open(
        "src/data/json/{name}.json".format(name=file_name), "w", encoding="utf-8"
    ) as file:
        json.dump(fetched_data, file, ensure_ascii=False, indent=4)


def json_fetch(file_name: str) -> any:
    BASE_PATH = "src/data/json/"
    selected_path = BASE_PATH + file_name + ".json"
    with open(selected_path) as read_file:
        data = json.load(read_file)
    return data


def cv_json_fetch(file_name: str) -> any:
    BASE_PATH = "src/data/json/cv/"
    selected_path = BASE_PATH + file_name + ".json"
    with open(selected_path) as read_file:
        data = json.load(read_file)
    return data


def cv_json_file_numbers_generator():
    number_list = []
    path = "src/data/json/cv"
    files = os.listdir(path)
    for file in files:
        number_list.append(int(file.replace(".json", "")))
    return number_list


if __name__ == "__main__":
    print(cv_json_file_numbers_generator())
