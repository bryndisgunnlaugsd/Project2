import os
import sqlite3
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import threading
import pika
import json

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
#tekur við niðurstöðu greiðslu fra rabbitmq
def process_payment(channel, method, properties, body):
    event = json.loads(body)
    product_id = event["productId"]
    conn = get_db()

    #ef greiðsla tokstminnkum lager og fratekið magn
    if method.exchange == "payment_success":
        conn.execute(
            """
            UPDATE products
            SET quantity = quantity -1,
                reserved = reserved -1
            WHERE id = ? AND reserved > 0
            """,
            (product_id,)
        )
    #ef virkaði ekki, losum við vöru
    elif method.exchange == "payment_failure":
        conn.execute(
            """
            UPDATE products
            SET reserved = reserved - 1
            WHERE id = ? AND reserved > 0
            """,
            (product_id,)
        )
    conn.commit()
    conn.close()
#staðfestir að buið se að vinna ur skilaboðum
    channel.basic_ack(delivery_tag=method.delivery_tag)
#tengist rabbitmq og hlustar eftir greiðslum
def start_consumer():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host="rabbitmq")
    )
    channel = connection.channel()
    exchanges = ["payment_success", "payment_failure"]
    for exchange in exchanges:
        channel.exchange_declare(
            exchange=exchange,
            exchange_type="fanout",
            durable=True

        )
#ser biðröð fyrir inventory service
        queue_name= f"inventory_{exchange}"
        channel.queue_declare(
            queue=queue_name,
            durable=True
        )
        channel.queue_bind(
            exchange=exchange,
            queue= queue_name
        )
        channel.basic_consume(
            queue = queue_name,
            on_message_callback=process_payment,
            auto_ack=False
        )
    channel.basic_qos(prefetch_count=1)
    print("InventoryService waiting for payments...", flush=True)
    channel.start_consuming()
#ræsir rabbitmq hlustun i ser þræði
@app.on_event("startup")
def start_rabbitmq():
    thread = threading.Thread(
        target=start_consumer,
        daemon=True
    )
    thread.start()