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
