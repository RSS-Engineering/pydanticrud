from unittest.mock import patch

from pydanticrud import BaseModel


class FalseBackend:
    @classmethod
    def get(cls, id):
        pass


class Model(BaseModel):
    id: int
    name: str
    total: float

    class Config:
        title = 'ModelTitle123'
        backend = FalseBackend


def test_model_has_backend_methods():
    assert hasattr(Model, 'get')


def test_model_backend_get():
    with patch.object(FalseBackend, 'get', return_value=dict(id=1, name='two', total=3.0)) as mock_get:
        m = Model.get(1)

        mock_get.assert_called_with(1)
        assert m.id == 1
        assert m.name == 'two'
        assert m.total == 3.0


def test_model_table_name_from_title():
    assert Model.get_table_name() == Model.Config.title.lower()


