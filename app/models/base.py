from typing import Optional
from uuid import uuid4, UUID

from pydantic import BaseModel
from rule_engine import Rule

from ..settings import backend
from ..cascade_types import FLAG_VALUE_TYPE
from ..exceptions import ConditionCheckFailed, RevisionMismatch


def build_update_condition(hash_key: str, key_value: FLAG_VALUE_TYPE):
    # Key(hash_key).eq(key_value)
    return f'{hash_key} == "{key_value}"'


def build_update_condition_with_revision(
        hash_key: str,
        key_value: FLAG_VALUE_TYPE,
        revision: UUID,
        field: Optional[str] = None,
        value: Optional[FLAG_VALUE_TYPE] = None,
        allow_create: bool = False):
    condition = build_update_condition(hash_key, key_value)

    # attr = Attr('revision')
    if isinstance(revision, UUID):
        # condition = condition and attr.eq(str(revision))
        condition += f' and revision == "{revision}"'

        if None not in (field, value):
            # condition = condition and Attr(field).ne(value)
            condition += f' and {field} != "{value}"'

    elif allow_create:
        # condition = condition and attr.not_exists()
        condition += f' and revision == NULL'
    else:
        # condition = False
        condition += f' and 1 == 0'

    return Rule(condition)


class UnversionedBaseModel(BaseModel):
    @classmethod
    def initialize(cls):
        backend.initialize(cls)

    @classmethod
    def get_table_name(cls) -> str:
        return cls.Config.title.lower()

    @classmethod
    def exists(cls):
        return backend.exists(cls)

    @classmethod
    def query(cls, condition: Rule):
        res = backend.query(cls, condition)
        return [cls.parse_obj(i) for i in res]

    @classmethod
    def get(cls, item_key: str):
        return cls.parse_obj(backend.get(cls, item_key))

    def save(self) -> bool:
        # Parse the new obj to trigger validation
        self.__class__.parse_obj(self.dict())

        # Maybe we should pass a conditional to the backend but for now the only place that uses it doesn't need it.
        return backend.save(self)

    @classmethod
    def delete(cls, item_key: str):
        backend.delete(cls, item_key)


class VersionedBaseModel(UnversionedBaseModel):
    revision: Optional[UUID]

    @classmethod
    def update_value(cls, key: str, field: str, value: FLAG_VALUE_TYPE, revision: Optional[UUID]) -> UUID:
        new_revision = uuid4()
        hash_key = cls.Config.hash_key

        condition = build_update_condition_with_revision(
            hash_key,
            key,
            revision,
            field,
            value
        )

        try:
            if backend.update_value(cls, key, {"revision": new_revision, field: value}, condition):
                return new_revision
            return revision
        except ConditionCheckFailed:
            raise RevisionMismatch('Provided revision is out of date' if revision else 'Must provide a revision')

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
            if not backend.save(self, condition):
                self.revision = old_revision
        except ConditionCheckFailed:
            self.revision = old_revision
            raise RevisionMismatch('Provided revision is out of date' if old_revision else 'Must provide a revision')
