from pydantic import BaseModel
from rule_engine import Rule

from .settings import get_backend


class UnversionedBaseModel(BaseModel):
    @classmethod
    def initialize(cls):
        get_backend(cls).initialize()

    @classmethod
    def get_table_name(cls) -> str:
        return cls.Config.title.lower()

    @classmethod
    def exists(cls):
        return get_backend(cls).exists()

    @classmethod
    def query(cls, condition: Rule):
        res = get_backend(cls).query(condition)
        return [cls.parse_obj(i) for i in res]

    @classmethod
    def get(cls, item_key: str):
        return cls.parse_obj(get_backend(cls).get(item_key))

    def save(self) -> bool:
        # Parse the new obj to trigger validation
        self.__class__.parse_obj(self.dict())

        # Maybe we should pass a conditional to the backend but for now the only place that uses it doesn't need it.
        return get_backend(self.__class__).save(self)

    @classmethod
    def delete(cls, item_key: str):
        get_backend(cls).delete(item_key)
