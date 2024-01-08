from unittest.mock import patch

from pydanticrud import BaseModel
from pydantic import ConfigDict


class FalseBackend:
    def __init__(self, cfg):
        self.cfg = cfg

    @classmethod
    def get(cls, id):
        pass

    @classmethod
    def query(cls, id):
        pass


class Model(BaseModel):
    id: int
    name: str
    total: float
    model_config = ConfigDict(title="ModelTitle123", backend=FalseBackend)


def test_model_has_backend_methods():
    assert hasattr(Model, "get")


def test_model_backend_get():
    with patch.object(
        FalseBackend, "get", return_value=dict(id=1, name="two", total=3.0)
    ) as mock_get:
        m = Model.get(1)

        mock_get.assert_called_with(1)
        assert m.id == 1
        assert m.name == "two"
        assert m.total == 3.0


def test_model_backend_query():
    with patch.object(
        FalseBackend, "query", return_value=[dict(id=1, name="two", total=3.0)]
    ) as mock_query:
        m = Model.query(2)

        mock_query.assert_called_with(2)
        assert m.count is None  # In this case the backend did not provide a total count.
        assert len(m[:]) == 1  # .. but we can cast to a list and get that length.
        assert m[0].id == 1
        assert m[0].name == "two"
        assert m[0].total == 3.0


def test_model_table_name_from_title():
    assert Model.get_table_name() == Model.model_config.get("title").lower()
