from datetime import datetime, timedelta
import random


used_names = set()

first_names = ("John", "Andy", "Joe", "Bob", "Alice", "Jane", "Bart")
last_names = ("Johnson", "Smith", "Williams", "Doe")


def random_datetime():
    return datetime(
        random.randint(2005, 2021),
        random.randint(1, 12),
        random.randint(1, 28),
        random.randint(1, 12),
        random.randint(1, 59),
        0
    )


def random_unique_name():
    global used_names

    name = ""
    while not name or name in used_names:
        name = f"{random.choice(first_names)} {random.choice(last_names)}"
    used_names.add(name)
    return name


def future_datetime(**kwargs):
    return datetime.now() + timedelta(**kwargs)
