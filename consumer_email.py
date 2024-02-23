from bson import ObjectId

import pika

from time import sleep

from model import Contacts

import mongo_connect


def send_email(email: str) -> None:
    sleep(1)
    print(f"Email to {email} sent.")


def callback(ch, method, properties, body) -> None:
    _id = ObjectId(body)
    contacts = Contacts.objects(id=_id)
    for contact in contacts:
        send_email(contact.email)
        contact.update(is_message_sent=True)
    print(f" [x] Done: {method.delivery_tag}")
    ch.basic_ack(delivery_tag=method.delivery_tag)


def main() -> None:
    credentials = pika.PlainCredentials("guest", "guest")
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host="localhost", port=5672, credentials=credentials)
    )
    channel = connection.channel()

    channel.queue_declare(queue="email_queue", durable=True)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue="email_queue", on_message_callback=callback)
    print(" [*] Waiting for messages. To exit press CTRL+C")
    channel.start_consuming()


if __name__ == "__main__":
    main()
