import os
import sqlite3
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

DB_PATH = "data/inventory.db"
os.makedirs("data", exist_ok=True)

app = FastAPI()

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


# Búum til products töfluna ef hún er ekki til
conn = get_db()
conn.execute("""
    CREATE TABLE IF NOT EXISTS products (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        merchantId INTEGER,
        productName TEXT,
        price REAL,
        quantity INTEGER,
        reserved INTEGER DEFAULT 0
    )
""")
conn.commit()
conn.close()


class Product(BaseModel):
    merchantId: int
    productName: str
    price: float
    quantity: int


@app.post("/products", status_code=201)
def create_product(product: Product):
    conn = get_db()
    cur = conn.execute(
        "INSERT INTO products (merchantId, productName, price, quantity, reserved) VALUES (?, ?, ?, ?, 0)",
        (product.merchantId, product.productName, product.price, product.quantity),
    )
    conn.commit()
    product_id = cur.lastrowid
    conn.close()
    return product_id


@app.get("/products/{id}")
def get_product(id: int):
    conn = get_db()
    row = conn.execute("SELECT * FROM products WHERE id = ?", (id,)).fetchone()
    conn.close()

    if row is None:
        raise HTTPException(status_code=404, detail="Product does not exist")

    return {
        "merchantId": row["merchantId"],
        "productName": row["productName"],
        "price": row["price"],
        "quantity": row["quantity"],
        "reserved": row["reserved"],
    }


# Internal endapunktur sem OrderService notar til að taka vöru frá
@app.post("/products/{id}/reserve")
def reserve_product(id: int):
    conn = get_db()
    row = conn.execute("SELECT * FROM products WHERE id = ?", (id,)).fetchone()
    if row is None:
        conn.close()
        raise HTTPException(status_code=404, detail="Product does not exist")

    # Tekur bara frá ef það er til laust eintak
    cur = conn.execute(
        "UPDATE products SET reserved = reserved + 1 WHERE id = ? AND quantity - reserved > 0",
        (id,),
    )
    conn.commit()
    updated = cur.rowcount
    conn.close()

    if updated == 0:
        raise HTTPException(status_code=400, detail="Product is sold out")

    return {"message": "Product reserved"}