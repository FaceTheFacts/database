import sys
from json_handler import json_generator

sys.path.append("src")
from json_handler import json_fetch


def fraction_membership_json_generator():
    data_list = []
    data = json_fetch("candidacy_mandate")
    for datum in data:
        is_fraction_membership = datum.get("fraction_membership")
        if is_fraction_membership:
            # fraction_memberships include only one dictionary in a list
            fraction_membership = datum["fraction_membership"][0]
            data_list.append(fraction_membership)

    json_generator(data_list, "fraction_membership")


def fraction_membership_fraction_json_generator() -> None:
    data_list = []
    data = json_fetch("fraction_membership")
    for datum in data:
        new_data = {
            "id": datum["id"],
            "entity_type": datum["entity_type"],
            "label": datum["label"],
            "fraction_id": datum["fraction"]["id"],
            "valid_from": datum["valid_from"],
            "valid_until": datum["valid_until"],
        }
        data_list.append(new_data)
    json_generator(data_list, "fraction_membership_fraction")


def electoral_data_json_generator() -> None:
    data_list = []
    data = json_fetch("candidacy_mandate")
    for datum in data:
        is_electoral_data = datum.get("electoral_data")
        if is_electoral_data:
            electoral_data = datum["electoral_data"]
            data_list.append(electoral_data)

    json_generator(data_list, "electoral_data")
    print("electoral_data has {} dictionaries in total".format(len(data_list)))


def electoral_data_ids_json_generator() -> None:
    data_list = []
    data = json_fetch("electoral_data")
    for datum in data:
        new_data = {
            "id": datum["id"],
            "entity_type": datum["entity_type"],
            "label": datum["label"],
            "electoral_list_id": datum["electoral_list"]["id"]
            if datum["electoral_list"]
            else None,
            "list_position": datum["list_position"],
            "constituency_id": datum["constituency"]["id"]
            if datum["constituency"]
            else None,
            "constituency_result": datum["constituency_result"],
            "constituency_result_count": datum["constituency_result_count"],
            "mandate_won": datum["mandate_won"],
        }
        data_list.append(new_data)
    json_generator(data_list, "electoral_data_ids")


def candidacy_mandate_ids_json_generator() -> None:
    data_list = []
    data = json_fetch("candidacy_mandate")
    for datum in data:
        new_data = {
            "id": datum["id"],
            "entity_type": datum["entity_type"],
            "label": datum["label"],
            "api_url": datum["api_url"],
            "id_external_administration": datum["id_external_administration"],
            "id_external_administration_description": datum[
                "id_external_administration_description"
            ],
            "type": datum["type"],
            "parliament_period_id": datum["parliament_period"]["id"]
            if datum["parliament_period"]
            else None,
            "politician_id": datum["politician"]["id"] if datum["politician"] else None,
            # Some dict don't include party itsself
            "party_id": datum["party"]["id"] if datum.get("party") else None,
            "start_date": datum["start_date"],
            "end_date": datum["end_date"],
            "info": datum["info"],
            "electoral_data_id": datum["electoral_data"]["id"]
            if datum["electoral_data"]
            else None,
            # Some dict don't include fraction_membership itsself
            "fraction_membership_id": datum["fraction_membership"][0]["id"]
            if datum.get("fraction_membership")
            else None,
        }
        data_list.append(new_data)
    json_generator(data_list, "candidacy_mandate_ids")


if __name__ == "__main__":
    # fraction_membership_json_generator()
    # fraction_membership_fraction_json_generator()
    # electoral_data_json_generator()
    # electoral_data_ids_json_generator()
    candidacy_mandate_ids_json_generator()
