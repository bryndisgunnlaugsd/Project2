import os
import sqlite3
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

DB_PATH = "data/merchants.db"
os.makedirs("data", exist_ok=True)

app = FastAPI()

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


# Búum til merchants töfluna ef hún er ekki til
conn = get_db()
conn.execute("""
    CREATE TABLE IF NOT EXISTS merchants (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT,
        ssn TEXT,
        email TEXT,
        phoneNumber TEXT,
        allowsDiscount INTEGER
    )
""")
conn.commit()
conn.close()


class Merchant(BaseModel):
    name: str
    ssn: str
    email: str
    phoneNumber: str
    allowsDiscount: bool


@app.post("/merchants", status_code=201)
def create_merchant(merchant: Merchant):
    conn = get_db()
    cur = conn.execute(
        "INSERT INTO merchants (name, ssn, email, phoneNumber, allowsDiscount) VALUES (?, ?, ?, ?, ?)",
        (merchant.name, merchant.ssn, merchant.email, merchant.phoneNumber, int(merchant.allowsDiscount)),
    )
    conn.commit()
    merchant_id = cur.lastrowid
    conn.close()
    return merchant_id


@app.get("/merchants/{id}")
def get_merchant(id: int):
    conn = get_db()
    row = conn.execute("SELECT * FROM merchants WHERE id = ?", (id,)).fetchone()
    conn.close()

    if row is None:
        raise HTTPException(status_code=404, detail="Merchant does not exist")

    return {
        "name": row["name"],
        "ssn": row["ssn"],
        "email": row["email"],
        "phoneNumber": row["phoneNumber"],
        "allowsDiscount": bool(row["allowsDiscount"]),
    }