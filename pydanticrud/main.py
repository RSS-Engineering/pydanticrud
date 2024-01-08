from pydantic import BaseModel as PydanticBaseModel
from pydantic._internal._model_construction import ModelMetaclass


class CrudMetaClass(ModelMetaclass):
    def __new__(mcs, name, bases, namespace, **kwargs):
        cls = super().__new__(mcs, name, bases, namespace, **kwargs)
        if hasattr(cls, "model_config") and "backend" in cls.model_config:
            cls.__backend__ = cls.model_config["backend"](cls)
        return cls


class IterableResult:
    def __init__(self, cls, records, count=None):
        self.records = [cls.parse_obj(i) for i in records]
        self.count = count  # None indicates "unknown"

        self._current_index = 0

    def __len__(self):
        return self.count

    def __iter__(self):
        return self

    def __getitem__(self, indices):
        return self.records.__getitem__(indices)

    def __next__(self):
        try:
            member = self.records[self._current_index]
            self._current_index += 1
            return member
        except IndexError:
            self._current_index = 0
            raise StopIteration


class BaseModel(PydanticBaseModel, metaclass=CrudMetaClass):
    @classmethod
    def initialize(cls):
        return cls.__backend__.initialize()

    @classmethod
    def get_table_name(cls) -> str:
        return cls.model_config.get("title").lower()

    @classmethod
    def exists(cls) -> bool:
        return cls.__backend__.exists()

    @classmethod
    def query(cls, *args, **kwargs):
        res = cls.__backend__.query(*args, **kwargs)
        if not isinstance(res, IterableResult):
            res = IterableResult(cls, res)
        return res

    @classmethod
    def count(cls, *args, **kwargs):
        return cls.__backend__.count(*args, **kwargs)

    @classmethod
    def get(cls, *args, **kwargs):
        return cls.model_validate(cls.__backend__.get(*args, **kwargs))

    def save(self) -> bool:
        # Parse the new obj to trigger validation
        self.__class__.model_validate(self.model_dump(by_alias=True))

        # Maybe we should pass a conditional to the backend but for now the only place that uses it doesn't need it.
        return self.__class__.__backend__.save(self)

    @classmethod
    def delete(cls, *args, **kwargs):
        cls.__backend__.delete(*args, **kwargs)

    @classmethod
    def batch_save(cls, *args, **kwargs):
        return cls.__backend__.batch_save(*args, **kwargs)
