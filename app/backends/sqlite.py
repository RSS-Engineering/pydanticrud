from typing import Optional, Dict
import json
from decimal import Decimal
from sqlite3 import connect

from pydantic import BaseSettings
from rule_engine import Rule, ast

from ..exceptions import DoesNotExist, ConditionCheckFailed


class Settings(BaseSettings):
    DB: str = ':memory'
    INITIALIZE: bool = True

    class Config:
        env_prefix = 'SQLITE_'


def expression_to_condition(expr, key_name: Optional[str] = None):
    if isinstance(expr, ast.LogicExpression):
        left, l_params = expression_to_condition(expr.left, key_name)
        right, r_params = expression_to_condition(expr.right, key_name)
        if expr.type == 'and':
            return f"({left} AND {right})", l_params + r_params
        if expr.type == 'or':
            return f"({left} OR {right})", l_params + r_params
    if isinstance(expr, ast.ComparisonExpression):
        left, l_params = expression_to_condition(expr.left, key_name)
        right, r_params = expression_to_condition(expr.right, key_name)
        if expr.type == 'eq':
            return f"{left} = {right}" if right is not None else f"{left} IS NULL", l_params + r_params
        if expr.type == 'ne':
            return f"{left} != {right}" if right is not None else f"{left} IS NOT NULL", l_params + r_params
    if isinstance(expr, ast.SymbolExpression):
        if expr.name == 'null':
            return None, ()
        return expr.name, ()
    if isinstance(expr, ast.NullExpression):
        return None, ()
    if isinstance(expr, ast.StringExpression):
        return "?", tuple([expr.value])
    # if isinstance(expr, ast.ContainsExpression):
    #     container = expression_to_condition(expr.container, key_name)
    #     member = expression_to_condition(expr.member, key_name)
    #     return container.contains(member)
    raise NotImplementedError


def rule_to_sqlite_expression(rule: Rule, key_name: Optional[str] = None):
    return expression_to_condition(rule.statement.expression, key_name)


SQLITE_TYPE_MAP = {
    'int': 'INTEGER',
    'decimal': 'TEXT',
    'double': 'REAL',
    'string': 'TEXT',
    'bool': 'INTEGER',
    'object': 'JSON',
    'array': 'JSON'
}

SERIALIZE_MAP = {
    'int': int,
    'decimal': str,
    'double': str,
    'string': str,
    'bool': lambda b: 1 if b else 0,
    'object': json.dumps,
    'array': json.dumps,
}


def do_nothing(x):
    return x


DESERIALIZE_MAP = {
    'int': do_nothing,
    'decimal': Decimal,
    'double': Decimal,
    'string': do_nothing,
    'bool': bool,
    'object': json.loads,
    'array': json.loads,
}


class Backend:
    def __init__(self):
        self.settings = Settings()
        self._conn = connect(self.settings.DB, isolation_level=None)

    @staticmethod
    def sql_field_defs(cls):
        schema = cls.schema()
        return {
            k: SQLITE_TYPE_MAP.get(v['type'], 'TEXT')
            for k, v in schema['properties'].items()
        }

    def initialize(self, cls):
        field_defs = self.sql_field_defs(cls)
        field_defs[cls.Config.hash_key] += ' PRIMARY KEY'
        fields = ', '.join(f"{k} {v}" for k, v in field_defs.items())
        c = self._conn.cursor()
        c.execute(f'''CREATE TABLE IF NOT EXISTS {cls.get_table_name()} ({fields})''')
        c.close()
        return True

    def exists(self, cls):
        c = self._conn.cursor()
        c.execute("select sql from sqlite_master where type = 'table' and name = ?;", [cls.get_table_name()])
        res = bool(c.fetchone())
        c.close()
        return res

    def query(self, cls, expression):
        # fields = tuple(self.sql_field_defs(cls).keys())
        c = self._conn.cursor()
        expression, params = rule_to_sqlite_expression(expression)
        c.execute(f"select * from {cls.get_table_name()} where {expression};", params)
        res = list(c.fetchall())
        c.close()
        schema = cls.schema()['properties']
        fields = list(schema.keys())
        return [
            {k: DESERIALIZE_MAP[schema[k]['type']](v) for k, v in zip(fields, rec)}
            for rec in res
        ]

    def get(self, cls, item_key):
        c = self._conn.cursor()
        c.execute(f"select * from {cls.get_table_name()} where {cls.Config.hash_key} = ?;", [item_key])
        res = c.fetchone()
        c.close()
        if not res:
            raise DoesNotExist
        schema = cls.schema()['properties']
        fields = list(schema.keys())
        return {k: DESERIALIZE_MAP[schema[k]['type']](v) for k, v in zip(fields, res)}

    def save(self, item, condition: Optional[Rule] = None) -> bool:
        table_name = item.get_table_name()
        hash_key = item.Config.hash_key
        key = getattr(item, hash_key)
        fields_def = self.sql_field_defs(item.__class__)
        fields = tuple(fields_def.keys())

        schema = item.schema()['properties']
        item_data = item.dict()
        values = tuple([SERIALIZE_MAP[schema[field]['type']](item_data[field]) for field in fields])
        try:
            old_item = self.get(item.__class__, key)
            if not condition.matches(old_item):
                raise ConditionCheckFailed()

            qs = ', '.join(f"{field} = ?" for field in fields)
            if condition:
                condition_expr, condition_params = rule_to_sqlite_expression(condition)
            else:
                condition_expr = f"{hash_key} = ?"
                condition_params = tuple(key)

            self._conn.execute(f"UPDATE {table_name} SET {qs} WHERE {condition_expr};", values + condition_params)
            return True
        except DoesNotExist:
            qs = ','.join(['?'] * len(fields))
            self._conn.execute(f"insert into {table_name} values ({qs})", values)
        return True

    def delete(self, cls, item_key: str):
        table_name = cls.get_table_name()
        self._conn.execute(f"DELETE FROM {table_name} WHERE {cls.Config.hash_key} = ?;", [item_key])
