from typing import Dict
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


class Model(BaseModel):
    id: int
    name: str
    sigfig: Decimal
    updated: datetime
    data: Dict[int, int] = None

    class Config:
        title = "ModelTitle123"
        hash_key = "name"
        backend = DynamoDbBackend
        endpoint = "http://localhost:18002"


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
    data = dict(
        id=1,
        name="two",
        updated=datetime.fromisoformat("2018-08-01T16:01:00+00:00"),
        sigfig=Decimal("4.001"),
    )
    a = Model.parse_obj(data)
    a.save()
    b = Model.get("two")
    assert b.dict() == a.dict()


def test_query(dynamo):
    data1 = dict(
        id=1,
        name="two",
        updated=datetime.fromisoformat("2018-08-01T16:01:00+00:00"),
        sigfig=Decimal("4.001"),
    )
    data2 = dict(
        id=2,
        name="four",
        updated=datetime.fromisoformat("2019-08-01T16:01:00+00:00"),
        sigfig=Decimal("4.001"),
        data={1: 0},
    )
    Model.parse_obj(data1).save()
    Model.parse_obj(data2).save()
    Model.parse_obj(
        dict(
            id=3,
            name="six",
            updated=datetime.fromisoformat("2020-08-01T16:01:00+00:00"),
            sigfig=Decimal("4.001"),
        )
    ).save()
    Model.parse_obj(
        dict(
            id=4,
            name="eight",
            updated=datetime.fromisoformat("2021-08-01T16:01:00+00:00"),
            sigfig=Decimal("4.001"),
        )
    ).save()
    res = Model.query(Rule("name == 'two'"))
    data = {m.id: m.dict() for m in res}

    data1["data"] = None  # This is a default value and should be populated as such
    assert data == {1: data1}

    res = Model.query(Rule("name == 'four'"))
    data = {m.id: m.dict() for m in res}
    assert data == {2: data2}

    res = Model.query(Rule(f"updated <= '{datetime.fromisoformat('2019-08-01T16:01:00+00:00')}'"))
    data = {m.id: m.dict() for m in res}
    assert data == {1: data1, 2: data2}
