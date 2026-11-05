
import os
import sqlite3
import pika
import json

from datetime import datetime

DB_PATH = "data/payments.db"
os.makedirs("data", exist_ok=True)


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


conn = get_db()
conn.execute("""
    CREATE TABLE IF NOT EXISTS payments (
        orderId INTEGER PRIMARY KEY,
        status TEXT
    )
""")
conn.commit()
conn.close()


# Athugum hvort kortanúmerið standist Luhn algorithm
def luhn_check(card_number):
    if not card_number.isdigit():
        return False

    total = 0
    reverse_digits = card_number[::-1]

    for i, digit in enumerate(reverse_digits):
        number = int(digit)

        if i % 2 == 1:
            number *= 2
            if number > 9:
                number -= 9

        total += number

    return total % 10 == 0


# Validate kortaupplýsingar
def validate_credit_card(credit_card):
    card_number = str(credit_card.get("cardNumber", ""))
    month = credit_card.get("expirationMonth")
    year = credit_card.get("expirationYear")
    cvc = str(credit_card.get("cvc", ""))

    # Athugum kortanúmerið með Luhn algorithm
    if not luhn_check(card_number):
        return False

    # Mánuður þarf að vera á bilinu 1-12
    if not isinstance(month, int) or not 1 <= month <= 12:
        return False

    # Ár þarf að vera fjórar tölur
    if not isinstance(year, int) or not 1000 <= year <= 9999:
        return False

    # Athugum hvort kortið sé útrunnið
    today = datetime.now()

    if year < today.year:
        return False

    if year == today.year and month < today.month:
        return False

    # CVC þarf að vera þrjár tölur
    if not cvc.isdigit() or len(cvc) != 3:
        return False

    return True

# RabbitMQ 
RABBITMQ_HOST = "rabbitmq"


# Sendum event um niðurstöðu greiðslunnar
def publish_payment_result(channel, event, success):
    if success:
        exchange = "payment_success"
    else:
        exchange = "payment_failure"

    channel.exchange_declare(
        exchange=exchange,
        exchange_type="fanout",
        durable=True
    )

    channel.basic_publish(
        exchange=exchange,
        routing_key="",
        body=json.dumps(event),
        properties=pika.BasicProperties(delivery_mode=2)
    )


# Vinnum úr Order-Created event
def process_order(channel, method, properties, body):
    event = json.loads(body)

    order_id = event["orderId"]
    credit_card = event["creditCard"]


    # Athugum hvort búið sé að vinna úr þessari pöntun
    conn = get_db()
    existing_payment = conn.execute(
        "SELECT * FROM payments WHERE orderId = ?",
        (order_id,)
    ).fetchone()
    conn.close()

    if existing_payment is not None:
        print(f"Order {order_id} already processed", flush=True)
        channel.basic_ack(delivery_tag=method.delivery_tag)
        return

    # Athugum hvort kortaupplýsingarnar séu gildar
    success = validate_credit_card(credit_card)

    # Vistum niðurstöðuna
    conn = get_db()
    conn.execute(
        "INSERT INTO payments (orderId, status) VALUES (?, ?)",
        (order_id, "success" if success else "failed"),
    )
    conn.commit()
    conn.close()

    # Sendum áfram upplýsingar um greiðsluna
    payment_event = {
        "orderId": order_id,
        "productId": event["productId"],
        "buyerEmail": event["buyerEmail"],
        "merchantEmail": event["merchantEmail"],
    }

    publish_payment_result(channel, payment_event, success)


    channel.basic_ack(delivery_tag=method.delivery_tag)


def start_consumer():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST)
    )

    channel = connection.channel()

    channel.exchange_declare(
        exchange="order_created",
        exchange_type="fanout",
        durable=True
    )

    # Queue fyrir PaymentService
    channel.queue_declare(queue="payment_orders", durable=True)

    channel.queue_bind(
        exchange="order_created",
        queue="payment_orders"
    )

    channel.basic_qos(prefetch_count=1)

    channel.basic_consume(
        queue="payment_orders",
        on_message_callback=process_order,
        auto_ack=False
    )

    print("PaymentService waiting for orders...", flush=True)

    channel.start_consuming()


if __name__ == "__main__":
    start_consumer()
