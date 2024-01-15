from typing import Dict, List
from decimal import Decimal
from datetime import datetime

import pytest

from pydanticrud import BaseModel, SqliteBackend
from rule_engine import Rule
from pydantic import ConfigDict


class Model(BaseModel):
    id: int
    value: int
    name: str
    total: float
    timestamp: datetime
    sigfig: Decimal
    enabled: bool
    data: Dict[str, str]
    items: List[int]

    class db_config:
        title = "ModelTitle123"
        hash_key = "id"
        backend = SqliteBackend
        database = ":memory:"


@pytest.fixture()
def model_in_db():
    if not Model.exists():
        Model.initialize()


def model_data_generator():
    import random

    return dict(
        id=random.randint(0, 100000),
        value=random.randint(0, 100000),
        name=random.choice(("bob", "alice", "john", "jane")),
        total=round(random.random(), 9),
        timestamp=datetime(2007, 2, 2, 2, 2, 0),
        sigfig=Decimal(str(random.random())[:8]),
        enabled=random.choice((True, False)),
        data=dict(a=str(random.randint(0, 1000))),
        items=[random.randint(0, 100000), random.randint(0, 100000), random.randint(0, 100000)],
    )


def test_exist_checks_for_table_existence():
    conn = Model.__backend__._conn

    assert not Model.exists()
    conn.execute(f"CREATE TABLE IF NOT EXISTS {Model.get_table_name()} (id INTEGER PRIMARY KEY)")
    assert Model.exists()
    conn.execute(f"DROP TABLE {Model.get_table_name()}")
    assert not Model.exists()


def test_initialize_creates_table():
    assert not Model.exists()
    Model.initialize()
    c = Model.__backend__._conn.execute(
        "select sql from sqlite_master where type = 'table' and name = ?;", [Model.get_table_name()]
    )
    assert bool(c.fetchone())


def test_save_and_get(model_in_db):
    data = model_data_generator()
    a = Model.model_validate(data)
    assert a.model_dump() == data
    a.save()
    b = Model.get(data["id"])
    assert b.dict() == data


def test_rule_parsing_gives_sql_equality():
    assert Model.__backend__._rule_to_sqlite_expression(Rule(f"id == 3")) == ("id = ?", (3,))


def test_rule_parsing_gives_sql_gt():
    assert Model.__backend__._rule_to_sqlite_expression(Rule(f"id > 3")) == ("id > ?", (3,))


def test_rule_parsing_gives_sql_like():
    assert Model.__backend__._rule_to_sqlite_expression(Rule(f"'o' in name")) == (
        "name like ?",
        ("%o%",),
    )


def test_rule_parsing_errors_on_querying_nonnative_fields():
    with pytest.raises(SyntaxError):
        Model.__backend__._rule_to_sqlite_expression(Rule(f"3 in items"))


def test_rule_parsing_errors_on_querying_nonexistent_fields():
    with pytest.raises(SyntaxError):
        Model.__backend__._rule_to_sqlite_expression(Rule(f"nonexistent == 3"))


def test_query(model_in_db):
    data1 = model_data_generator()
    data1["id"] = 1
    data2 = model_data_generator()
    data2["id"] = 2
    data3 = model_data_generator()
    data3["id"] = 1234
    Model.model_validate(data1).save()
    Model.model_validate(data2).save()
    Model.model_validate(data3).save()
    for r in range(0, 10):
        _data = model_data_generator()
        _data["id"] += 3
        Model.model_validate(_data).save()
    res = Model.query(Rule(f"id < 3"))
    data = {m.id: m.dict() for m in res}
    assert data == {1: data1, 2: data2}
