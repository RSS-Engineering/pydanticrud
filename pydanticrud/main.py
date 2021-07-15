from pydantic import BaseModel as PydanticBaseModel
from pydantic.main import ModelMetaclass
from rule_engine import Rule


class CrudMetaClass(ModelMetaclass):
    def __new__(mcs, name, bases, namespace, **kwargs):
        print(bases)
        cls = super().__new__(mcs, name, bases, namespace, **kwargs)
        if hasattr(cls.__config__, 'backend'):
            cls.__backend__ = cls.__config__.backend

        return cls


class BaseModel(PydanticBaseModel, metaclass=CrudMetaClass):
    @classmethod
    def initialize(cls):
        pass

    @classmethod
    def get_table_name(cls) -> str:
        return cls.Config.title.lower()

    @classmethod
    def exists(cls) -> bool:
        return cls.__backend__.exists()

    @classmethod
    def query(cls, condition: Rule):
        res = cls.__backend__.query(condition)
        return [cls.parse_obj(i) for i in res]

    @classmethod
    def get(cls, item_key):
        return cls.parse_obj(cls.__backend__.get(item_key))

    def save(self) -> bool:
        # Parse the new obj to trigger validation
        self.__class__.parse_obj(self.dict())

        # Maybe we should pass a conditional to the backend but for now the only place that uses it doesn't need it.
        return self.__class__.__backend__.save(self)

    @classmethod
    def delete(cls, item_key):
        cls.__backend__.delete(item_key)
