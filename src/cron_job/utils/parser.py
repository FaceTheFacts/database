from typing import TypedDict, Optional, Any
from ...utils.file import read_json
from ...utils.fetch import load_entity
from ...db.session import Session
from ...models.politician import Politician

LEFT = "Linke"
GREEN = "Grüne"
PARTIES = {"CDU", "CSU", "SPD", "FDP", GREEN, LEFT, "AfD"}
CUSTOM_PARTY_NAME = {
    "Bündnis 90/Die Grünen": GREEN,
    "DIE LINKE": LEFT,
}


PERIOD_POSITION_TABLE = {
    130: "mecklenburg-vorpommern",
    129: "berlin",
    128: "general",
}


class PartyStyle(TypedDict):
    id: int
    display_name: str
    foreground_color: str
    background_color: str
    border_color: Optional[str]


class Statement(TypedDict):
    id: int
    statement: str
    topic_id: str


class Position(TypedDict):
    id: int
    position: str
    reason: Optional[str]
    politician_id: int
    parliament_period_id: int
    position_statement_id: int


def gen_party_styles_map(api_parties: list[Any]) -> dict[int, PartyStyle]:
    party_colors = read_json("src/static/party_colors.json")
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


def gen_statements(period_id: int) -> list[Statement]:
    file_path = f"src/static/{PERIOD_POSITION_TABLE[period_id]}-assumptions.json"
    assumptions = read_json(file_path)
    statements: list[Statement] = []
    for id in assumptions:
        statement_id = str(period_id) + str(id)
        statement: Statement = {
            "id": int(statement_id),
            "statement": assumptions[id]["statement"],
            "topic_id": assumptions[id]["topic_id"],
        }
        statements.append(statement)
    return statements


def gen_positions(period_id: int) -> list[Position]:
    file_path = f"src/static/{PERIOD_POSITION_TABLE[period_id]}-positions.json"
    position_data = read_json(file_path)
    api_politicians = load_entity("politicians")
    politician_ids: set[int] = set([politician["id"] for politician in api_politicians])
    positions: list[Position] = []
    for politician_id in position_data:
        if int(politician_id) not in politician_ids:
            print(politician_id)
        else:
            for item in position_data[politician_id]:
                assumption_id: str = str(next(iter(item)))
                position_id = str(period_id) + str(politician_id) + assumption_id
                statement_id = str(period_id) + assumption_id
                position: Position = {
                    "id": int(position_id),
                    "position": item[assumption_id]["position"],
                    "reason": item[assumption_id]["reason"]
                    if "reason" in item[assumption_id]
                    else None,
                    "politician_id": int(politician_id),
                    "parliament_period_id": period_id,
                    "position_statement_id": int(statement_id),
                }
                positions.append(position)
    return positions
