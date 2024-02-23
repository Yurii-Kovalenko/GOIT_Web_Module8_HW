from json import load as load_json

FILE_AUTHORS = "./json/authors.json"

FILE_QUOTES = "./json/quotes.json"


def read_from_file(filename):
    with open(filename, "r") as fr:
        unpacked = load_json(fr)
    return unpacked


def read_authors():
    return read_from_file(FILE_AUTHORS)


def read_quotes():
    return read_from_file(FILE_QUOTES)
