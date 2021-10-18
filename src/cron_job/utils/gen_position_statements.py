from typing import TypedDict
from ...utils.file import read_json

PERIOD_POSITION_TABLE = {
    130: "mecklenburg-vorpommern",
    129: "berlin",
    128: "general",
}


class Statement(TypedDict):
    id: int
    statement: str
    topic_id: str


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


def gen_positions(period_id: int):
    file_path = f"src/static/{PERIOD_POSITION_TABLE[period_id]}-positions.json"
    position_data = read_json(file_path)
    positions = []
    for politician_id in position_data:
        for item in position_data[politician_id]:
            assumption_id: str = str(next(iter(item)))
            position_id = str(period_id) + str(politician_id) + assumption_id
            statement_id = str(period_id) + assumption_id
            position = {
                "id": position_id,
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
