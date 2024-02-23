from auxiliary_functions import read_authors, read_quotes

from models import Authors, Quotes

import my_connect


if __name__ == "__main__":
    authors_json = read_authors()
    quotes_json = read_quotes()
    authors = []
    for author_data in authors_json:
        authors.append(
            Authors(
                fullname=author_data["fullname"],
                born_date=author_data["born_date"],
                born_location=author_data["born_location"],
                description=author_data["description"],
            )
        )
    for quote_data in quotes_json:
        for author in authors:
            if quote_data["author"] == author.fullname:
                Quotes(
                    tags=quote_data["tags"], author=author, quote=quote_data["quote"]
                ).save()
