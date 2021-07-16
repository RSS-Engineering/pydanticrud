from decimal import Decimal

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

    class Config:
        title = "ModelTitle123"
        hash_key = "name"
        backend = DynamoDbBackend
        endpoint = "http://localhost:8002"


@pytest.fixture(scope="module")
def dynamo():
    client = docker.from_env()
    c = client.containers.run(
        "dwmkerr/dynamodb",
        command=[" -jar", "DynamoDBLocal.jar", "-port", "8002"],
        ports={"8002": "8002"},
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
    data = dict(id=1, name="two", sigfig=Decimal("4.001"))
    a = Model.parse_obj(data)
    a.save()
    b = Model.get("two")
    assert b.dict() == a.dict()


def test_query(dynamo):
    data1 = dict(id=1, name="two", sigfig=Decimal("4.001"))
    data2 = dict(id=2, name="four", sigfig=Decimal("4.001"))
    Model.parse_obj(data1).save()
    Model.parse_obj(data2).save()
    Model.parse_obj(dict(id=3, name="six", sigfig=Decimal("4.001"))).save()
    Model.parse_obj(dict(id=4, name="eight", sigfig=Decimal("4.001"))).save()
    res = Model.query(Rule("name == 'two'"))
    data = {m.id: m.dict() for m in res}
    assert data == {1: data1}
