from datetime import datetime
import random

import docker
import pytest

from pydanticrud import OpenSearchBackend, BaseModel, DynamoDbBackend
from tests.random_values import random_unique_name, random_datetime


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

class SimpleKeyModel(BaseModel):
    id: int
    value: int
    name: str
    total: float
    timestamp: datetime
    enabled: bool
    body: str

    class Config:
        title = "ModelTitle123"
        region = "us-west-2"
        backend = DynamoDbBackend
        hash_key = "name"
        endpoint = "http://localhost:18002"
        global_indexes = {"by-id": ("id",)}

        os_index = "id"
        host = 'localhost'
        opensearch = OpenSearchBackend


def simple_model_data_generator(**kwargs):
    data = dict(
        id=random.randint(0, 100000),
        value=random.randint(0, 100000),
        name=random_unique_name(),
        total=round(random.random(), 9),
        timestamp=random_datetime(),
        enabled=random.choice((True, False)),
        body='This is test for opensearch backend will it work'
    )
    data.update(kwargs)
    return data


def test_initialize():
    assert SimpleKeyModel.initialize()


def test_simple_model_data_save():
    data = simple_model_data_generator()
    SimpleKeyModel.initialize()
    response = SimpleKeyModel.parse_obj(data).save()


def test_simple_model_data_query():
    query = 'opensearch'
    SimpleKeyModel.initialize()
    response = SimpleKeyModel.query(query)


def test_simple_model_data_get():
    query = 'opensearch'
    SimpleKeyModel.initialize()
    response = SimpleKeyModel.get(query)


def test_simple_model_data_delete(dynamo):
    data = simple_model_data_generator()
    SimpleKeyModel.initialize()
    id = SimpleKeyModel.parse_obj(data).save()
    SimpleKeyModel.delete(id=id)
