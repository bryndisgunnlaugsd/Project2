import os
import json
import sqlite3
from typing import Optional

import pika
import requests
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

DB_PATH = "data/orders.db"
os.makedirs("data", exist_ok=True)

MERCHANT_URL = "http://merchantservice:8001"
BUYER_URL = "http://buyerservice:8002"
INVENTORY_URL = "http://inventoryservice:8003"
RABBITMQ_HOST = "rabbitmq"

app = FastAPI()


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


# Búum til orders töfluna ef hún er ekki til
conn = get_db()
conn.execute("""
    CREATE TABLE IF NOT EXISTS orders (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        productId INTEGER,
        merchantId INTEGER,
        buyerId INTEGER,
        cardNumber TEXT,
        totalPrice REAL
    )
""")
conn.commit()
conn.close()


class CreditCard(BaseModel):
    cardNumber: str
    expirationMonth: int
    expirationYear: int
    cvc: int

class Order(BaseModel):
    productId: int
    merchantId: int
    buyerId: int
    creditCard: CreditCard
    discount: Optional[float] = None


def mask_card(card_number):
    # Sama lengd og númerið nema bara síðustu 4 sjást, * fyrir hina sem á undan koma
    return "*" * (len(card_number) - 4) + card_number[-4:]


def publish_order_created(event):
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    channel.exchange_declare(exchange="order_created", exchange_type="fanout", durable=True)
    channel.basic_publish(
        exchange="order_created",
        routing_key="",
        body=json.dumps(event),
        properties=pika.BasicProperties(delivery_mode=2),  # persistent skilaboð
    )
    connection.close()


@app.post("/orders", status_code=201)
def create_order(order: Order):
    # Merchant does not exist
    res = requests.get(f"{MERCHANT_URL}/merchants/{order.merchantId}")
    if res.status_code != 200:
        raise HTTPException(status_code=400, detail="Merchant does not exist")
    merchant = res.json()

    # Buyer does not exist
    res = requests.get(f"{BUYER_URL}/buyers/{order.buyerId}")
    if res.status_code != 200:
        raise HTTPException(status_code=400, detail="Buyer does not exist")
    buyer = res.json()

    # Product does not exist
    res = requests.get(f"{INVENTORY_URL}/products/{order.productId}")
    if res.status_code != 200:
        raise HTTPException(status_code=400, detail="Product does not exist")
    product = res.json()

    # Product is sold out
    if product["quantity"] - product["reserved"] <= 0:
        raise HTTPException(status_code=400, detail="Product is sold out")

    # Product does not belong to merchant
    if product["merchantId"] != order.merchantId:
        raise HTTPException(status_code=400, detail="Product does not belong to merchant")

    # athugum hvort afslatturinn se a bilinu 0 til 1
    discount = order.discount 
    if discount is None:
        discount = 0

    if discount < 0 or discount > 1:
        raise HTTPException(status_code=400, detail="invalid discount")
    #athuga hvort seljandi leyfi discoutn
    if not merchant["allowsDiscount"] and discount != 0:
        raise HTTPException(status_code=400, detail="merchant doesnt allow discount")

    # Taka vöruna frá
    res = requests.post(f"{INVENTORY_URL}/products/{order.productId}/reserve")
    if res.status_code != 200:
        raise HTTPException(status_code=400, detail="Product is sold out")

    # 8. Vista kaupin
    total_price = product["price"] * (1 - discount)

    conn = get_db()
    cur = conn.execute(
        "INSERT INTO orders (productId, merchantId, buyerId, cardNumber, totalPrice) VALUES (?, ?, ?, ?, ?)",
        (order.productId, order.merchantId, order.buyerId,
         mask_card(order.creditCard.cardNumber), total_price),
    )
    conn.commit()
    order_id = cur.lastrowid
    conn.close()

    # 9. Senda order created event
    event = {
        "orderId": order_id,
        "productId": order.productId,
        "productName": product["productName"],
        "totalPrice": total_price,
        "buyerEmail": buyer["email"],
        "merchantEmail": merchant["email"],
        "creditCard": order.creditCard.model_dump(),
    }
    publish_order_created(event)

    return order_id


@app.get("/orders/{id}")
def get_order(id: int):
    conn = get_db()
    row = conn.execute("SELECT * FROM orders WHERE id = ?", (id,)).fetchone()
    conn.close()

    if row is None:
        raise HTTPException(status_code=404, detail="Order does not exist")

    return {
        "productId": row["productId"],
        "merchantId": row["merchantId"],
        "buyerId": row["buyerId"],
        "cardNumber": row["cardNumber"],
        "totalPrice": row["totalPrice"],
    }