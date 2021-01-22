from typing import Optional
from uuid import uuid4, UUID

from pydantic import BaseModel
from rule_engine import Rule

from ..settings import get_backend
from ..cascade_types import FLAG_VALUE_TYPE
from ..exceptions import ConditionCheckFailed, RevisionMismatch


def build_update_condition(hash_key: str, key_value: FLAG_VALUE_TYPE):
    return f'{hash_key} == "{key_value}"'


def build_update_condition_with_revision(
        hash_key: str,
        key_value: FLAG_VALUE_TYPE,
        revision: UUID,
        field: Optional[str] = None,
        value: Optional[FLAG_VALUE_TYPE] = None,
        allow_create: bool = False):
    condition = build_update_condition(hash_key, key_value)

    if isinstance(revision, UUID):
        condition += f' and revision == "{revision}"'

        if None not in (field, value):
            condition += f' and {field} != "{value}"'

    elif allow_create:
        condition += f' and revision == null'
    else:
        condition += f' and 1 == 0'

    return Rule(condition)


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
        get_backend(cls).delete(cls, item_key)


class VersionedBaseModel(UnversionedBaseModel):
    revision: Optional[UUID]

    def save(self):
        # Parse the new obj to trigger validation
        self.__class__.parse_obj(self.dict())

        hash_key = self.Config.hash_key
        key_value = getattr(self, hash_key)
        old_revision = self.revision
        self.revision = str(uuid4())

        condition = build_update_condition_with_revision(
            hash_key,
            key_value,
            old_revision,
            allow_create=True
        )

        try:
            if not get_backend(self.__class__).save(self, condition):
                self.revision = old_revision
        except ConditionCheckFailed:
            self.revision = old_revision
            raise RevisionMismatch('Provided revision is out of date' if old_revision else 'Must provide a revision')
