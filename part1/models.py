from mongoengine import EmbeddedDocument, Document

from mongoengine.fields import (
    EmbeddedDocumentField,
    ListField,
    StringField,
)


class Authors(EmbeddedDocument):
    fullname = StringField()
    born_date = StringField()
    born_location = StringField()
    description = StringField()


class Quotes(Document):
    tags = ListField(StringField())
    author = EmbeddedDocumentField(Authors)
    quote = StringField()
