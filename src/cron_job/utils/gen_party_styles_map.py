from typing import TypedDict, Optional, Any

from ...utils.file import read_json
from ...utils.fetch import load_entity


LEFT = "Linke"
GREEN = "Grüne"
PARTIES = {"CDU", "CSU", "SPD", "FDP", GREEN, LEFT, "AfD"}
CUSTOM_PARTY_NAME = {
    "Bündnis 90/Die Grünen": GREEN,
    "DIE LINKE": LEFT,
}

PARTY_COLORS_PATH = "src/static/party_colors.json"


class PartyStyle(TypedDict):
    id: int
    display_name: str
    foreground_color: str
    background_color: str
    border_color: Optional[str]


def gen_party_styles_map(api_parties: list[Any]) -> dict[int, PartyStyle]:
    party_colors = read_json(PARTY_COLORS_PATH)
    party_names = [party_color["displayName"].lower() for party_color in party_colors]

    party_styles_map: dict[int, PartyStyle] = {}
    for api_party in api_parties:
        label = api_party["label"]
        has_ref = label in CUSTOM_PARTY_NAME
        party_id = api_party["id"]
        party_name = CUSTOM_PARTY_NAME[label] if has_ref else label
        try:
            idx = party_names.index(party_name.lower())
            border_color = party_colors[idx].get("borderColor")
            party_styles_map[party_id] = {
                "id": party_id,
                "display_name": party_name,
                "foreground_color": party_colors[idx]["foregroundColor"],
                "background_color": party_colors[idx]["backgroundColor"],
                "border_color": border_color,
            }
        except ValueError:
            pass

    return party_styles_map
