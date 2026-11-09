import os
import json
import pika

from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
#tengjast rabitmq
RABBITMQ_HOST = "rabbitmq"
#sendir tölvupost með sendgrid
def send_email(to_email, subject, body):
    message = Mail(
        from_email=os.environ["SENDGRID_SENDER_EMAIL"],
        to_emails=to_email,
        subject=subject,
        plain_text_content=body
    )
    #sækir api lykilinn og sendir postinn
    sg = SendGridAPIClient(os.environ["SENDGRID_API_KEY"])
    response = sg.send(message)
    #athugar hvort sendgrid hafi tekið við postinum
    if response.status_code != 202:
        raise Exception("email unaccepted by sendgrid")
    print(f"Email sent to {to_email}", flush=True)
#tekur við skilaboðum fra rabbitmq
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
    #postur þegar greiðsla tekst
    elif method.exchange == "payment_success":
        subject = "Order has been purchased"
        message = f"Order {order_id} has been successfully purchased"
    #postur þegar greiðsla mistekst
    elif method.exchange == "payment_failure":
        subject = "Order purchase failed"
        message = f"Order {order_id} purchase has failed"
    #hafna skilaboðum sem þekkjum ekki
    else:
        channel.basic_nack(
            delivery_tag=method.delivery_tag,
            requeue = False
        )
        return
    #sendir post a kaupanda og seljanda
    send_email(event["buyerEmail"], subject, message)
    send_email(event["merchantEmail"], subject, message)
    #staðfestir að buið er að vinna ur skilaboðunum
    channel.basic_ack(delivery_tag=method.delivery_tag)
#setur upp tengingu við rabbitmq
def start_consumer():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST)
    )
    channel = connection.channel()
    #viljum fa þessa þrjar tegundir
    exchanges = [
        "order_created",
        "payment_success",
        "payment_failure"
    ]
    for exchange in exchanges:
        #byr til exchange i rabbitmq
        channel.exchange_declare(
            exchange=exchange,
            exchange_type="fanout",
            durable=True
        )
        #byr til biðroð fyrir skilaboðin
        queue_name = f"email_{exchange}"
        channel.queue_declare(
            queue=queue_name,
            durable=True
        )
        #tengir biðröð við exchange
        channel.queue_bind(
            exchange=exchange,
            queue=queue_name
        )
        #hlustar eftir skilaboðum i biðroðini
        channel.basic_consume(
            queue=queue_name,
            on_message_callback=process_event,
            auto_ack=False
        )
    #tekur eitt ostaðfest skilaboð i einu
    channel.basic_qos(prefetch_count=1)
    print("EmailService waiting for events...", flush=True)
    #hlusta eftir skilaboðum
    channel.start_consuming()

if __name__ == "__main__":
    start_consumer()

