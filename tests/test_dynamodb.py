from typing import Dict, List
from decimal import Decimal
from datetime import datetime

import docker
from pydanticrud import BaseModel, DynamoDbBackend
import pytest
from rule_engine import Rule


class FalseBackend:
    @classmethod
    def get(cls, id):
        pass


used_names = set()


class Model(BaseModel):
    id: int
    value: int
    name: str
    total: float
    timestamp: datetime
    sigfig: Decimal
    enabled: bool
    data: Dict[int, int] = None
    items: List[int]

    class Config:
        title = "ModelTitle123"
        hash_key = "name"
        backend = DynamoDbBackend
        endpoint = "http://localhost:18002"


def model_data_generator(**kwargs):
    global used_names
    import random

    first_names = ("John", "Andy", "Joe", "Bob", "Alice", "Jane", "Bart")
    last_names = ("Johnson", "Smith", "Williams", "Doe")

    name = ""
    while not name or name in used_names:
        name = f"{random.choice(first_names)} {random.choice(last_names)}"
    used_names.add(name)

    data = dict(
        id=random.randint(0, 100000),
        value=random.randint(0, 100000),
        name=name,
        total=round(random.random(), 9),
        timestamp=datetime(random.randint(2005, 2021), random.randint(1, 12), 2, 2, 2, 0),
        sigfig=Decimal(str(random.random())[:8]),
        enabled=random.choice((True, False)),
        data={random.randint(0, 1000): random.randint(0, 1000)},
        items=[random.randint(0, 100000), random.randint(0, 100000), random.randint(0, 100000)],
    )
    data.update(kwargs)
    return data


@pytest.fixture(scope="module")
def dynamo():
    client = docker.from_env()
    c = client.containers.run(
        "dwmkerr/dynamodb",
        command=[" -jar", "DynamoDBLocal.jar", "-port", "18002"],
        ports={"18002": "18002"},
        remove=True,
        detach=True,
    )
    try:
        yield c
    finally:
        c.stop()


def test_initialize_creates_table(dynamo):
    assert not Model.exists()
    Model.initialize()
    assert Model.exists()


def test_save_and_get(dynamo):
    data = model_data_generator()
    a = Model.parse_obj(data)
    a.save()
    try:
        b = Model.get(data["name"])
        assert b.dict() == a.dict()
    finally:
        Model.delete(data["name"])


def test_query(dynamo):
    presets = [dict(name="Jerry"), dict(name="Hermione"), dict(), dict(), dict()]
    data = [datum for datum in [model_data_generator() for i in presets]]
    del data[0]["data"]  # We need to have no data to ensure that default values work
    for datum in data:
        Model.parse_obj(datum).save()

    res = Model.query(Rule(f"name == '{data[0]['name']}'"))
    res_data = {m.name: m.dict() for m in res}
    data[0]["data"] = None  # This is a default value and should be populated as such
    assert res_data == {data[0]["name"]: data[0]}

    res = Model.query(Rule(f"name == '{data[1]['name']}'"))
    res_data = {m.name: m.dict() for m in res}
    assert res_data == {data[1]["name"]: data[1]}

    data_by_timestamp = data[:]
    data_by_timestamp.sort(key=lambda d: d["timestamp"])
    res = Model.query(Rule(f"timestamp <= '{data_by_timestamp[2]['timestamp']}'"))
    res_data = {m.name: m.dict() for m in res}
    assert res_data == {d["name"]: d for d in data_by_timestamp[:2]}

    res = Model.query(Rule(f"'{data[2]['items'][1]}' in items"))
    res_data = {m.name: m.dict() for m in res}
    assert res_data == {data[2]["name"]: data[2]}
