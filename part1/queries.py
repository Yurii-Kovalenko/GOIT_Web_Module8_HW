"""
Запити регістронезалежні, пробіли - також не завада.
Пошук можливий в усіх трьох варіантах (name: , tag: , tags: ) за початковими буквами(буквою).
__in з __iregex не знайшов як сумістити, тому обробку tags: зробив таким чином.
"""

from models import Quotes

import my_connect

EXIT = "exit"


def print_quote(quote: Quotes) -> None:
    print(f"Quote: {quote.quote}")
    print(f"Author: {quote.author.fullname}.")
    print(f"tags: {', '.join(quote.tags)}.")
    print("-" * 50)
    print()


def name_handler(command: list[str]) -> None:
    name = command[1].strip()
    quotes = Quotes.objects(author__fullname__iregex=f"^{name}")
    for quote in quotes:
        print_quote(quote)


def tag_handler(command: list[str]) -> None:
    tag = command[1].strip()
    quotes = Quotes.objects(tags__iregex=f"^{tag}")
    for quote in quotes:
        print_quote(quote)


def tags_handler(command: list[str]) -> None:
    tags = [tag.strip().lower() for tag in command[1].strip().split(",")]
    quotes_set = set()
    for tag in tags:
        quotes = Quotes.objects(tags__iregex=f"^{tag}")
        for quote in quotes:
            quotes_set.add(quote)
    for quote in quotes_set:
        print_quote(quote)


def exit_handler(command: list[str]) -> None:
    print("Goodbye!")


HANDLERS = {
    "name": name_handler,
    "tag": tag_handler,
    "tags": tags_handler,
    EXIT: exit_handler,
}


def invalid_command_handler(command: list[str]) -> None:
    print("The command was not found. Please enter valid command.")


if __name__ == "__main__":
    command = [""]
    while command[0].lower() != EXIT:
        command = input("Enter the command: ").split(":")
        HANDLERS.get(command[0].lower(), invalid_command_handler)(command)
