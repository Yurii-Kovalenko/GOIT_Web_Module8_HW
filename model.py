from mongoengine import Document

from mongoengine.fields import StringField, BooleanField


class Contacts(Document):
    fullname = StringField()
    email = StringField()
    phone = StringField()
    favorite_email = BooleanField()
    is_message_sent = BooleanField()
