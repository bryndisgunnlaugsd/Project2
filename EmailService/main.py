import os
import json
import pika

from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

RABBITMQ_HOST = "rabbitmq"

def send_email(to_email, subject, body):
    message = Mail(
        from_email=os.environ["SENDGRID_SENDER_EMAIL"],
        to_emails=to_email,
        subject=subject,
        plain_text_content=body
    )
    sg = SendGridAPIClient(os.environ["SENDGRID_API_KEY"])
    response = sg.send(message)

    if response.status_code != 202:
        raise Exception("Email was not accepted by SendGrid")
    print(f"Email sent to {to_email}", flush=True)

def process_event(channel, method, properties, body):
    event = json.loads(body)
    order_id = event["orderId"]
    if method.exchange == "order_created":
        subject = "Order has been created"
        message = (
            f"Order ID: {order_id}\n"
            f"Product: {event['productName']}\n"
            f"Price: {event['totalPrice']}"
        )

    elif method.exchange == "payment_success":
        subject = "Order has been purchased"
        message = f"Order {order_id} has been successfully purchased"

    elif method.exchange == "payment_failure":
        subject = "Order purchase failed"
        message = f"Order {order_id} purchase has failed"

    else:
        channel.basic_nack(
            delivery_tag=method.delivery_tag,
            requeue = False
        )
        return

    send_email(event["buyerEmail"], subject, message)
    send_email(event["merchantEmail"], subject, message)

    channel.basic_ack(delivery_tag=method.delivery_tag)

def start_consumer():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST)
    )
    channel = connection.channel()
    exchanges = [
        "order_created",
        "payment_success",
        "payment_failure"
    ]
    for exchange in exchanges:
        channel.exchange_declare(
            exchange=exchange,
            exchange_type="fanout",
            durable=True
        )

        queue_name = f"email_{exchange}"
        channel.queue_declare(
            queue=queue_name,
            durable=True
        )
        channel.queue_bind(
            exchange=exchange,
            queue=queue_name
        )

        channel.basic_consume(
            queue=queue_name,
            on_message_callback=process_event,
            auto_ack=False
        )
    channel.basic_qos(prefetch_count=1)
    print("EmailService waiting for events...", flush=True)
    channel.start_consuming()

if __name__ == "__main__":
    start_consumer()

