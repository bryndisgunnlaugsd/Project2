import os
import sqlite3
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

DB_PATH = "data/buyers.db"
os.makedirs("data", exist_ok=True)

app = FastAPI()

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


# Búum til buyers töfluna ef hún er ekki til
conn = get_db()
conn.execute("""
    CREATE TABLE IF NOT EXISTS buyers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT,
        ssn TEXT,
        email TEXT,
        phoneNumber TEXT
    )
""")
conn.commit()
conn.close()


class Buyer(BaseModel):
    name: str
    ssn: str
    email: str
    phoneNumber: str


@app.post("/buyers", status_code=201)
def create_buyer(buyer: Buyer):
    conn = get_db()
    cur = conn.execute(
        "INSERT INTO buyers (name, ssn, email, phoneNumber) VALUES (?, ?, ?, ?)",
        (buyer.name, buyer.ssn, buyer.email, buyer.phoneNumber),
    )
    conn.commit()
    buyer_id = cur.lastrowid
    conn.close()
    return buyer_id


@app.get("/buyers/{id}")
def get_buyer(id: int):
    conn = get_db()
    row = conn.execute("SELECT * FROM buyers WHERE id = ?", (id,)).fetchone()
    conn.close()

    if row is None:
        raise HTTPException(status_code=404, detail="Buyer does not exist")

    return {
        "name": row["name"],
        "ssn": row["ssn"],
        "email": row["email"],
        "phoneNumber": row["phoneNumber"],
    }