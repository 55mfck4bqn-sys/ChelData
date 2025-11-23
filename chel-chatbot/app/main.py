import os
import json
import psycopg2
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

PG_CONN_STR = os.getenv("SUPABASE_DB_URL")

SYSTEM_PROMPT = """
You are an analytics assistant for an EA Sports NHL Pro Clubs team.

The data you can use lives in a PostgreSQL database with these tables:

TABLE matches
- match_id BIGINT PRIMARY KEY
- club_id BIGINT
- opponent_club_id BIGINT
- goals_for INT
- goals_against INT
- result TEXT
- match_type TEXT
- played_at TIMESTAMP

TABLE players
- match_id BIGINT
- club_id BIGINT
- player_id BIGINT
- player_name TEXT
- position TEXT
- goals INT
- assists INT
- points INT
- score INT
- result TEXT

You MUST call run_sql when a user asks any statistics question.
You may only run SELECT queries.
Never modify or delete data.
Keep queries simple and readable.
"""


# Attach frontend folder
app.mount("/static", StaticFiles(directory="app/static"), name="static")


@app.get("/")
def root():
    return FileResponse("app/static/index.html")


tools = [
    {
        "type": "function",
        "function": {
            "name": "run_sql",
            "description": "Run a read-only SQL query against the NHL stats database and return rows as JSON.",
            "parameters": {
                "type": "object",
                "properties": {
                    "sql": {
                        "type": "string",
                        "description": "A read-only SQL SELECT query."
                    }
                },
                "required": ["sql"]
            },
        },
    }
]


def execute_sql(sql: str):
    lowered = sql.lower()
    if any(bad in lowered for bad in ["delete", "update", "insert", "drop", "alter"]):
        raise ValueError("Illegal SQL")

    conn = psycopg2.connect(PG_CONN_STR)
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            colnames = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
            return [dict(zip(colnames, r)) for r in rows]
    finally:
        conn.close()


class Msg(BaseModel):
    message: str


@app.post("/chat")
def chat(req: Msg):

    first = client.chat.completions.create(
        model="gpt-4.1",
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": req.message}
        ],
        tools=tools,
        tool_choice="auto",
    )

    msg = first.choices[0].message

    # Tool call?
    if msg.tool_calls:
        call = msg.tool_calls[0]
        if call.function.name == "run_sql":
            args = json.loads(call.function.arguments)
            sql = args["sql"]

            rows = execute_sql(sql)

            second = client.chat.completions.create(
                model="gpt-4.1",
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": req.message},
                    msg,
                    {
                        "role": "tool",
                        "tool_call_id": call.id,
                        "name": "run_sql",
                        "content": json.dumps(rows),
                    },
                ]
            )

            return {"answer": second.choices[0].message.content}

    return {"answer": msg.content}
