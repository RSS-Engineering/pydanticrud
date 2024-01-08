from typing import Dict, List, Optional, Union, Any
from decimal import Decimal
from datetime import datetime, timedelta, timezone
from uuid import uuid4, UUID
import random

import docker
from botocore.exceptions import ClientError
from pydantic import model_validator, ConfigDict, BaseModel as PydanticBaseModel, Field, ValidationError
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
    expires: datetime
    sigfig: Decimal
    enabled: bool
    data: Dict[int, int] = None
    items: List[int]
    hash: UUID
    model_config = ConfigDict(title="ModelTitle123", hash_key="name", ttl="expires", backend=DynamoDbBackend, endpoint="http://localhost:18002", global_indexes={"by-id": ("id",)})


class AliasKeyModel(BaseModel):
    id: int
    value: int
    name: str
    type_: str = Field(alias="type")

    @model_validator(mode="before")
    @classmethod
    def type_from_typ(cls, values):
        if 'typ' in values:
            values['type'] = values.pop('typ')
        return values
    model_config = ConfigDict(title="AliasTitle123", hash_key="name", backend=DynamoDbBackend, endpoint="http://localhost:18002")


class ComplexKeyModel(BaseModel):
    account: str
    sort_date_key: str
    expires: str
    category_id: int
    notification_id: str
    thread_id: str
    body: str = "some random string"
    model_config = ConfigDict(title="ComplexModelTitle123", hash_key="account", range_key="sort_date_key", backend=DynamoDbBackend, endpoint="http://localhost:18002", local_indexes={
        "by-category": ("account", "category_id"),
        "by-notification": ("account", "notification_id"),
        "by-thread": ("account", "thread_id")
    })


class Ticket(PydanticBaseModel):
    created_time: str
    number: str


class SomethingElse(PydanticBaseModel):
    herp: bool
    derp: int


class NestedModel(BaseModel):
    account: str
    sort_date_key: str
    expires: str
    ticket: Optional[Ticket]
    other: Union[Ticket, SomethingElse]
    model_config = ConfigDict(title="NestedModelTitle123", hash_key="account", range_key="sort_date_key", backend=DynamoDbBackend, endpoint="http://localhost:18002")


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
        expires=(datetime.utcnow() + timedelta(seconds=random.randint(0, 10))).replace(tzinfo=timezone.utc),
        sigfig=Decimal(str(random.random())[:8]),
        enabled=random.choice((True, False)),
        data={random.randint(0, 1000): random.randint(0, 1000)},
        items=[random.randint(0, 100000), random.randint(0, 100000), random.randint(0, 100000)],
        hash=uuid4()
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
            'number': str(random.randint(0, 1000))

        } if include_ticket else None,
        other=random.choice([
            {
                'created_time': random_datetime().isoformat(),
                'number': str(random.randint(0, 1000))

            }, {
                'herp': random.choice([True, False]),
                'derp': random.randint(0, 1000)

            }
        ])
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
    # for datum in data:
    #     print("I'm here")
    #     print(datum)
    #     SimpleKeyModel.model_validate(datum).save()
    try:
        yield data
    finally:
        for datum in data:
            SimpleKeyModel.delete(datum["name"])


@pytest.fixture(scope="module")
def complex_query_data(complex_table):
    record_count = 500
    presets = [dict()] * record_count
    accounts = [str(uuid4()) for i in range(4)]

    data = [
        complex_model_data_generator(account=accounts[i % 4], body="some random string", **p)
        for i, p in enumerate(presets)
    ]
    for datum in data:
        ComplexKeyModel.model_validate(datum).save()
    try:
        yield data
    finally:
        for datum in data:
            ComplexKeyModel.delete((datum[ComplexKeyModel.model_config.get("hash_key")], datum[ComplexKeyModel.model_config.get("range_key")]))


@pytest.fixture(scope="module")
def alias_query_data(alias_table):
    presets = [dict(name="Jerry"), dict(name="Hermione"), dict(), dict(), dict()]
    data = [datum for datum in [alias_model_data_generator(**i) for i in presets]]
    for datum in data:
        AliasKeyModel.model_validate(datum).save()
    try:
        yield data
    finally:
        for datum in data:
            AliasKeyModel.delete(datum["name"])


@pytest.fixture
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


@pytest.fixture
def nested_query_data_empty_ticket(nested_table):
    presets = [dict()] * 5
    data = [datum for datum in [nested_model_data_generator(include_ticket=False, **i) for i in presets]]
    for datum in data:
        NestedModel.model_validate(datum).save()
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


def test_save_ttl_field_is_float(dynamo, simple_query_data):
    """DynamoDB requires ttl fields to be a float in order to be successfully processed. Boto provides the ability to
    set a float via a decimal (but not a float strangely)."""

    key = simple_query_data[0]["name"]
    table = SimpleKeyModel.__backend__.get_table()
    resp = table.get_item(Key=SimpleKeyModel.__backend__._key_param_to_dict(key))
    expires_value = resp["Item"]["expires"]
    assert isinstance(expires_value, Decimal)
    assert datetime.utcfromtimestamp(float(expires_value)).replace(tzinfo=timezone.utc) == simple_query_data[0]["expires"]

    instance = SimpleKeyModel.get(key)
    assert instance.expires == simple_query_data[0]["expires"]


def test_query_with_hash_key_simple(dynamo, simple_query_data):
    res = SimpleKeyModel.query(Rule(f"name == '{simple_query_data[0]['name']}'"))
    res_data = {m.name: m.dict() for m in res}
    simple_query_data[0]["data"] = None  # This is a default value and should be populated as such
    assert res_data == {simple_query_data[0]["name"]: simple_query_data[0]}


def test_scan_errors_with_order(dynamo, simple_query_data):
    data_by_timestamp = simple_query_data[:]
    data_by_timestamp.sort(key=lambda d: d["timestamp"])
    with pytest.raises(ConditionCheckFailed,
                       match=r"Scans do not support reverse order."):
        SimpleKeyModel.query(order='desc')


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

    # Check that it works regardless of query attribute order
    res = ComplexKeyModel.query(
        Rule(f"sort_date_key == '{record['sort_date_key']}' and account == '{record['account']}'"))
    res_data = {(m.account, m.sort_date_key): m.dict() for m in res}
    assert res_data == {(record["account"], record["sort_date_key"]): record}


@pytest.mark.parametrize('order', ('asc', 'desc'))
def test_ordered_query_with_hash_key_complex(dynamo, complex_query_data, order):
    middle_record = complex_query_data[(len(complex_query_data)//2)]
    res = ComplexKeyModel.query(
        Rule(f"account == '{middle_record['account']}' and sort_date_key >= '{middle_record['sort_date_key']}'"),
        order=order
    )
    res_data = [(m.account, m.sort_date_key) for m in res]
    check_data = sorted([
        (m["account"], m["sort_date_key"])
        for m in complex_query_data
        if m["account"] == middle_record['account'] and m["sort_date_key"] >= middle_record['sort_date_key']
    ], reverse=order == 'desc')

    assert res_data == check_data


@pytest.mark.parametrize('order', ('asc', 'desc'))
def test_pagination_query_with_hash_key_complex(dynamo, complex_query_data, order):
    page_size = 2
    middle_record = complex_query_data[(len(complex_query_data)//2)]
    query_rule = Rule(f"account == '{middle_record['account']}' and sort_date_key >= '{middle_record['sort_date_key']}'")
    res = ComplexKeyModel.query(query_rule, order=order, limit=page_size)
    res_data = [(m.account, m.sort_date_key) for m in res]
    check_data = sorted([
        (m["account"], m["sort_date_key"])
        for m in complex_query_data
        if m["account"] == middle_record['account'] and m["sort_date_key"] >= middle_record['sort_date_key']
    ], reverse=order == 'desc')[:page_size]
    assert res_data == check_data
    assert res.last_evaluated_key == {"account": check_data[-1][0], "sort_date_key": check_data[-1][1]}

    res = ComplexKeyModel.query(query_rule, order=order, limit=page_size, exclusive_start_key=res.last_evaluated_key)
    res_data = [(m.account, m.sort_date_key) for m in res]
    check_data = sorted([
        (m["account"], m["sort_date_key"])
        for m in complex_query_data
        if m["account"] == middle_record['account'] and m["sort_date_key"] >= middle_record['sort_date_key']
    ], reverse=order == 'desc')[page_size:page_size*2]
    assert res_data == check_data


def test_pagination_query_with_index_complex(dynamo, complex_query_data):
    page_size = 2
    middle_record = complex_query_data[(len(complex_query_data)//2)]
    query_rule = Rule(f"account == '{middle_record['account']}' and category_id >= {middle_record['category_id']}")
    check_data = ComplexKeyModel.query(query_rule)
    res = ComplexKeyModel.query(query_rule, limit=page_size)
    res_data = [{"account": m.account, "category_id": m.category_id, "sort_date_key": m.sort_date_key} for m in res]
    # We only check for inclusion because the category index order is not going to be the same and since there are
    # multiple records per category, it's unknowable outside of the query response.
    assert all([r in check_data for r in res])
    assert len(res) == page_size
    assert res.last_evaluated_key == {"account": res_data[-1]["account"], "category_id": res_data[-1]["category_id"],
                                      "sort_date_key": res_data[-1]["sort_date_key"]}

    res = ComplexKeyModel.query(query_rule, limit=page_size, exclusive_start_key=res.last_evaluated_key)
    assert all([r in check_data for r in res])
    assert len(res) == page_size

def test_pagination_query_count(dynamo, complex_query_data):
    page_size = 2
    middle_record = complex_query_data[(len(complex_query_data)//2)]
    query_rule = Rule(f"account == '{middle_record['account']}' and category_id >= {middle_record['category_id']}")
    check_data = ComplexKeyModel.query(query_rule)
    res_count = ComplexKeyModel.count(query_rule)
    assert res_count == check_data.scanned_count

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
    res = NestedModel.query()
    for m in res:
        assert isinstance(m.ticket, Ticket)
        assert m.ticket.created_time is not None
        assert m.ticket.number is not None
        assert isinstance(m.other, (Ticket, SomethingElse))


def test_query_with_nested_model_optional(dynamo, nested_query_data_empty_ticket):
    res = NestedModel.query()
    assert all([m.ticket is None for m in res])


def test_query_alias_save(dynamo):
    presets = [dict(name="Jerry"), dict(name="Hermione"), dict(), dict(), dict()]
    data = [datum for datum in [alias_model_data_generator(**i) for i in presets]]
    AliasKeyModel.initialize()
    try:
        for datum in data:
            AliasKeyModel.model_validate(datum).save()
    except Exception as e:
        raise pytest.fail("Failed to save Alias model!")


def test_get_alias_model_data(dynamo, alias_query_data):
    data = alias_model_data_generator()
    res = AliasKeyModel.get(alias_query_data[0]['name'])
    assert res.dict(by_alias=True) == alias_query_data[0]


def test_get_simple_model_data_via_index(dynamo, simple_query_data):
    data = simple_model_data_generator()
    res = SimpleKeyModel.get(simple_query_data[0]['name'])
    assert res.dict(by_alias=True) == simple_query_data[0]
    res = SimpleKeyModel.get({"id": simple_query_data[0]['id']})
    assert res.dict(by_alias=True) == simple_query_data[0]


def test_get_complex_model_data_via_index(dynamo, complex_query_data):
    data = complex_model_data_generator()
    res = ComplexKeyModel.get((complex_query_data[0]['account'], complex_query_data[0]['sort_date_key']))
    assert res.dict(by_alias=True) == complex_query_data[0]
    res = ComplexKeyModel.get({"account": complex_query_data[0]['account'], "notification_id": complex_query_data[0]['notification_id']})
    assert res.dict(by_alias=True) == complex_query_data[0]


def test_alias_model_validator_ingest(dynamo):
    data = alias_model_data_generator()
    AliasKeyModel(**data)
    data["typ"] = data.pop("type")
    AliasKeyModel(**data)
    data.pop("typ")
    with pytest.raises(ValidationError):
        AliasKeyModel(**data)


def test_batch_write(dynamo, complex_table):
    response = {"UnprocessedItems": {}}
    data = [ComplexKeyModel.parse_obj(complex_model_data_generator()) for x in range(0, 10)]
    un_proc = ComplexKeyModel.batch_save(data)
    assert un_proc == response["UnprocessedItems"]
    res_get = ComplexKeyModel.get((data[0].account, data[0].sort_date_key))
    res_query = ComplexKeyModel.query(
        Rule(f"account == '{data[0].account}' and sort_date_key == '{data[0].sort_date_key}'")
    )
    assert res_get == data[0]
    assert res_query.count == 1
    assert res_query.records == [data[0]]


def test_message_batch_write_client_exception(dynamo, complex_table):
    data = [
        ComplexKeyModel.parse_obj(complex_model_data_generator(body="some big string" * 10000))
        for x in range(0, 2)
    ]
    with pytest.raises(ClientError) as exc:
        ComplexKeyModel.batch_save(data)
    assert (
        exc.value.response["Error"]["Message"] == "Item size has exceeded the maximum allowed size"
    )
