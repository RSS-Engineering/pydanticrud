from decimal import Decimal

from pydanticrud import BaseModel, SqliteBackend
from rule_engine import Rule

class FalseBackend:
    @classmethod
    def get(cls, id):
        pass


class Model(BaseModel):
    id: int
    name: str
    total: float
    sigfig: Decimal

    class Config:
        title = 'ModelTitle123'
        hash_key = 'id'
        backend = SqliteBackend
        database = ":memory:"


def test_initialize_creates_table():
    assert not Model.exists()
    Model.initialize()
    assert Model.exists()


def test_save_and_get():
    data = dict(id=1, name='two', total=3.0, sigfig=Decimal('4.001'))
    a = Model.parse_obj(data)
    a.save()
    b = Model.get(1)
    assert b.dict() == a.dict()


def test_query():
    data1 = dict(id=1, name='two', total=5.0, sigfig=Decimal('4.001'))
    data2 = dict(id=2, name='four', total=3.0, sigfig=Decimal('4.001'))
    Model.parse_obj(data1).save()
    Model.parse_obj(data2).save()
    Model.parse_obj(dict(id=3, name='six', total=3.0, sigfig=Decimal('4.001'))).save()
    Model.parse_obj(dict(id=4, name='eight', total=4.0, sigfig=Decimal('4.001'))).save()
    res = Model.query(Rule("id < 3"))
    data = {m.id: m.dict() for m in res}
    assert data == {
        1: data1,
        2: data2
    }
