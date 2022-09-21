from typing import Dict, List, Optional
from decimal import Decimal
from datetime import datetime
from uuid import uuid4
import random

import docker
from pydantic import BaseModel as PydanticBaseModel, Field
from pydanticrud import BaseModel, DynamoDbBackend, ConditionCheckFailed
import pytest
from pydanticrud.exceptions import DoesNotExist
from rule_engine import Rule

from .random_values import random_datetime, random_unique_name, future_datetime


class FalseBackend:
    @classmethod
    def get(cls, id):
        pass


class SimpleKeyModel(BaseModel):
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
        global_indexes = {"by-id": ("id",)}


class AliasKeyModel(BaseModel):
    id: int
    value: int
    name: str
    type_: str = Field(alias="type")

    class Config:
        title = "AliasTitle123"
        hash_key = "name"
        backend = DynamoDbBackend
        endpoint = "http://localhost:18002"


class ComplexKeyModel(BaseModel):
    account: str
    sort_date_key: str
    expires: str
    category_id: int
    notification_id: str
    thread_id: str

    class Config:
        title = "ComplexModelTitle123"
        hash_key = "account"
        range_key = "sort_date_key"
        backend = DynamoDbBackend
        endpoint = "http://localhost:18002"
        local_indexes = {
            "by-category": ("account", "category_id"),
            "by-notification": ("account", "notification_id"),
            "by-thread": ("account", "thread_id")
        }


class Ticket(PydanticBaseModel):
    created_time: str
    number: str


class NestedModel(BaseModel):
    account: str
    sort_date_key: str
    expires: str
    ticket: Optional[Ticket]

    class Config:
        title = "NestedModelTitle123"
        hash_key = "account"
        range_key = "sort_date_key"
        backend = DynamoDbBackend
        endpoint = "http://localhost:18002"


def alias_model_data_generator(**kwargs):
    data = dict(
        id=random.randint(0, 100000),
        value=random.randint(0, 100000),
        name=random_unique_name(),
        type="aliasType"
    )
    data.update(kwargs)
    return data


def simple_model_data_generator(**kwargs):
    data = dict(
        id=random.randint(0, 100000),
        value=random.randint(0, 100000),
        name=random_unique_name(),
        total=round(random.random(), 9),
        timestamp=random_datetime(),
        sigfig=Decimal(str(random.random())[:8]),
        enabled=random.choice((True, False)),
        data={random.randint(0, 1000): random.randint(0, 1000)},
        items=[random.randint(0, 100000), random.randint(0, 100000), random.randint(0, 100000)],
    )
    data.update(kwargs)
    return data


def complex_model_data_generator(**kwargs):
    data = dict(
        account=str(uuid4()),
        sort_date_key=random_datetime().isoformat(),
        expires=future_datetime(days=1, hours=random.randint(1, 12), minutes=random.randint(1, 58)).isoformat(),
        category_id=random.randint(1, 15),
        notification_id=str(uuid4()),
        thread_id=str(uuid4())
    )
    data.update(kwargs)
    return data


def nested_model_data_generator(include_ticket=True, **kwargs):
    data = dict(
        account=str(uuid4()),
        sort_date_key=random_datetime().isoformat(),
        expires=future_datetime(days=1, hours=random.randint(1, 12), minutes=random.randint(1, 58)).isoformat(),
        ticket={
            'created_time': random_datetime().isoformat(),
            'number': random.randint(0, 1000)

        } if include_ticket else None
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


@pytest.fixture(scope="module")
def simple_table(dynamo):
    if not SimpleKeyModel.exists():
        SimpleKeyModel.initialize()
        assert SimpleKeyModel.exists()
    return SimpleKeyModel


@pytest.fixture(scope="module")
def complex_table(dynamo):
    if not ComplexKeyModel.exists():
        ComplexKeyModel.initialize()
        assert ComplexKeyModel.exists()
    return ComplexKeyModel


@pytest.fixture(scope="module")
def nested_table(dynamo):
    if not NestedModel.exists():
        NestedModel.initialize()
        assert NestedModel.exists()
    return NestedModel


@pytest.fixture(scope="module")
def alias_table(dynamo):
    if not AliasKeyModel.exists():
        AliasKeyModel.initialize()
        assert AliasKeyModel.exists()
    return AliasKeyModel


@pytest.fixture(scope="module")
def simple_query_data(simple_table):
    presets = [dict(name="Jerry"), dict(name="Hermione"), dict(), dict(), dict()]
    data = [datum for datum in [simple_model_data_generator(**i) for i in presets]]
    del data[0]["data"]  # We need to have no data to ensure that default values work
    for datum in data:
        SimpleKeyModel.parse_obj(datum).save()
    try:
        yield data
    finally:
        for datum in data:
            SimpleKeyModel.delete(datum["name"])


@pytest.fixture(scope="module")
def complex_query_data(complex_table):
    presets = [dict()] * 20
    data = [datum for datum in [complex_model_data_generator(**i) for i in presets]]
    for datum in data:
        ComplexKeyModel.parse_obj(datum).save()
    try:
        yield data
    finally:
        for datum in data:
            ComplexKeyModel.delete((datum[ComplexKeyModel.Config.hash_key], datum[ComplexKeyModel.Config.range_key]))


@pytest.fixture(scope="module")
def alias_query_data(alias_table):
    presets = [dict(name="Jerry"), dict(name="Hermione"), dict(), dict(), dict()]
    data = [datum for datum in [alias_model_data_generator(**i) for i in presets]]
    for datum in data:
        AliasKeyModel.parse_obj(datum).save()
    try:
        yield data
    finally:
        for datum in data:
            AliasKeyModel.delete(datum["name"])

@pytest.fixture(scope="module")
def nested_query_data(nested_table):
    presets = [dict()] * 5
    data = [datum for datum in [nested_model_data_generator(**i) for i in presets]]
    for datum in data:
        nested_datum = NestedModel.parse_obj(datum)
        nested_datum.save()
    try:
        yield data
    finally:
        for datum in data:
            NestedModel.delete((datum[NestedModel.Config.hash_key], datum[NestedModel.Config.range_key]))


@pytest.fixture(scope="module")
def nested_query_data_optional(nested_table):
    presets = [dict()] * 5
    data = [datum for datum in [nested_model_data_generator(include_ticket=False, **i) for i in presets]]
    for datum in data:
        NestedModel.parse_obj(datum).save()
    try:
        yield data
    finally:
        for datum in data:
            NestedModel.delete((datum[NestedModel.Config.hash_key], datum[NestedModel.Config.range_key]))


def test_save_get_delete_simple(dynamo, simple_table):
    data = simple_model_data_generator()
    a = SimpleKeyModel.parse_obj(data)
    a.save()
    try:
        b = SimpleKeyModel.get(data["name"])
        assert b.dict() == a.dict()
    finally:
        SimpleKeyModel.delete(data["name"])

    with pytest.raises(DoesNotExist, match=f'modeltitle123 "{data["name"]}" does not exist'):
        SimpleKeyModel.get(data["name"])


def test_query_with_hash_key_simple(dynamo, simple_query_data):
    res = SimpleKeyModel.query(Rule(f"name == '{simple_query_data[0]['name']}'"))
    res_data = {m.name: m.dict() for m in res}
    simple_query_data[0]["data"] = None  # This is a default value and should be populated as such
    assert res_data == {simple_query_data[0]["name"]: simple_query_data[0]}


def test_query_errors_with_nonprimary_key_simple(dynamo, simple_query_data):
    data_by_timestamp = simple_query_data[:]
    data_by_timestamp.sort(key=lambda d: d["timestamp"])
    with pytest.raises(ConditionCheckFailed,
                       match=r"No keys in query expression. Use a filter expression or add an index."):
        SimpleKeyModel.query(Rule(f"timestamp <= '{data_by_timestamp[2]['timestamp']}'"))


def test_query_with_indexed_hash_key_simple(dynamo, simple_query_data):
    data_by_timestamp = simple_query_data[:]
    data_by_timestamp.sort(key=lambda d: d["timestamp"])
    res = SimpleKeyModel.query(Rule(f"id == {data_by_timestamp[0]['id']}"))
    res_data = {m.name: m.dict() for m in res}
    assert res_data == {data_by_timestamp[0]["name"]: data_by_timestamp[0]}


def test_query_with_indexed_hash_key_and_additional_nonindexed_key_simple(dynamo, simple_query_data):
    data_by_timestamp = simple_query_data[:]
    data_by_timestamp.sort(key=lambda d: d["timestamp"])
    with pytest.raises(ConditionCheckFailed,
                       match="Non-key attributes are not valid in the query expression. Use filter expression"):
        SimpleKeyModel.query(Rule(f"id == {data_by_timestamp[0]['id']} and timestamp == '"
                                  f"{data_by_timestamp[0]['timestamp']}'"))


def test_query_scan_simple(dynamo, simple_query_data):
    data_by_timestamp = simple_query_data[:]
    data_by_timestamp.sort(key=lambda d: d["timestamp"])
    res = SimpleKeyModel.query(filter_expr=Rule(f"timestamp <= '{data_by_timestamp[2]['timestamp']}'"))
    res_data = {m.name: m.dict() for m in res}
    assert res_data == {d["name"]: d for d in data_by_timestamp[:2]}


def test_query_scan_contains_simple(dynamo, simple_query_data):
    res = SimpleKeyModel.query(filter_expr=Rule(f"'{simple_query_data[2]['items'][1]}' in items"))
    res_data = {m.name: m.dict() for m in res}
    assert res_data == {simple_query_data[2]["name"]: simple_query_data[2]}


def test_save_get_delete_complex(dynamo, complex_table):
    data = complex_model_data_generator()
    a = ComplexKeyModel.parse_obj(data)
    a.save()
    try:
        b = ComplexKeyModel.get((data["account"], data["sort_date_key"]))
        assert b.dict() == a.dict()
    finally:
        ComplexKeyModel.delete((data["account"], data["sort_date_key"]))

    key = {
        "account": data["account"],
        "sort_date_key": data["sort_date_key"]
    }

    with pytest.raises(DoesNotExist, match=f'complexmodeltitle123 "{key}" does not exist'):
        ComplexKeyModel.get((data["account"], data["sort_date_key"]))


def test_query_with_hash_key_complex(dynamo, complex_query_data):
    record = complex_query_data[0]
    res = ComplexKeyModel.query(
        Rule(f"account == '{record['account']}' and sort_date_key == '{record['sort_date_key']}'"))
    res_data = {(m.account, m.sort_date_key): m.dict() for m in res}
    assert res_data == {(record["account"], record["sort_date_key"]): record}

    # Check that it works regardless of order
    res = ComplexKeyModel.query(
        Rule(f"sort_date_key == '{record['sort_date_key']}' and account == '{record['account']}'"))
    res_data = {(m.account, m.sort_date_key): m.dict() for m in res}
    assert res_data == {(record["account"], record["sort_date_key"]): record}


def test_query_errors_with_nonprimary_key_complex(dynamo, complex_query_data):
    data_by_expires = complex_query_data[:]
    data_by_expires.sort(key=lambda d: d["expires"])
    with pytest.raises(ConditionCheckFailed, match=r"No keys in expression. Enable scan or add an index."):
        ComplexKeyModel.query(Rule(f"notification_id <= '{data_by_expires[2]['notification_id']}'"))


def test_query_with_indexed_hash_key_complex(dynamo, complex_query_data):
    record = complex_query_data[0]
    res = ComplexKeyModel.query(Rule(f"account == '{record['account']}' and thread_id == '{record['thread_id']}'"))
    res_data = {(m.account, m.thread_id): m.dict() for m in res}
    assert res_data == {(record["account"], record["thread_id"]): record}

    res = ComplexKeyModel.query(Rule(f"thread_id == '{record['thread_id']}' and account == '{record['account']}'"))
    res_data = {(m.account, m.thread_id): m.dict() for m in res}
    assert res_data == {(record["account"], record["thread_id"]): record}


def test_query_scan_complex(dynamo, complex_query_data):
    data_by_expires = complex_query_data[:]
    data_by_expires.sort(key=lambda d: d["expires"])
    res = ComplexKeyModel.query(filter_expr=Rule(f"expires <= '{data_by_expires[2]['expires']}'"))
    res_data = {(m.account, m.sort_date_key): m.dict() for m in res}
    assert res_data == {(d["account"], d["sort_date_key"]): d for d in data_by_expires[:3]}


def test_query_with_nested_model(dynamo, nested_query_data):
    data_by_expires = nested_model_data_generator()
    res = NestedModel.query(filter_expr=Rule(f"expires <= '{data_by_expires['expires']}'"))
    res_data = [m.ticket for m in res]
    assert any(elem is not None for elem in res_data)


def test_query_with_nested_model_optional(dynamo, nested_query_data_optional):
    data_by_expires = nested_model_data_generator(include_ticket=False)
    res = NestedModel.query(filter_expr=Rule(f"expires <= '{data_by_expires['expires']}'"))
    res_data = [m.ticket for m in res]
    assert any(elem is None for elem in res_data)


def test_query_alias_save(dynamo):
    presets = [dict(name="Jerry"), dict(name="Hermione"), dict(), dict(), dict()]
    data = [datum for datum in [alias_model_data_generator(**i) for i in presets]]
    AliasKeyModel.initialize()
    try:
        for datum in data:
            AliasKeyModel.parse_obj(datum).save()
    except Exception as e:
        raise pytest.fail("Failed to save Alias model!")

def test_get_alias_model_data(dynamo, alias_query_data):
    data = alias_model_data_generator()
    res = AliasKeyModel.get(alias_query_data[0]['name'])
    assert res.dict(by_alias=True) == alias_query_data[0]


