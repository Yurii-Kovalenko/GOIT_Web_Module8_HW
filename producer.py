from faker import Faker

from random import choice

from bson import ObjectId

import pika

from model import Contacts

import mongo_connect


NUMBER_OF_CONTACTS = 20

TRUE_AND_FALSE = (True, False)


def fill_data() -> None:
    fake_data = Faker()
    for i in range(NUMBER_OF_CONTACTS):
        Contacts(
            fullname=fake_data.name(),
            email=fake_data.email(),
            phone=fake_data.phone_number(),
            favorite_email=choice(TRUE_AND_FALSE),
            is_message_sent=False,
        ).save()


def put_messages_in_queue() -> None:

    credentials = pika.PlainCredentials("guest", "guest")
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host="localhost", port=5672, credentials=credentials)
    )
    channel = connection.channel()

    channel.exchange_declare(exchange="forwarding_messages", exchange_type="direct")

    channel.queue_declare(queue="email_queue", durable=True)
    channel.queue_bind(exchange="forwarding_messages", queue="email_queue")

    channel.queue_declare(queue="phone_queue", durable=True)
    channel.queue_bind(exchange="forwarding_messages", queue="phone_queue")

    contacts = Contacts.objects(is_message_sent=False)
    for contact in contacts:
        message = contact.id.binary
        if contact.favorite_email:
            routing_key = "email_queue"
        else:
            routing_key = "phone_queue"

        channel.basic_publish(
            exchange="forwarding_messages",
            routing_key=routing_key,
            body=message,
            properties=pika.BasicProperties(
                delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
            ),
        )
    connection.close()


if __name__ == "__main__":
    fill_data()
    put_messages_in_queue()
