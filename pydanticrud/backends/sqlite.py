from typing import Optional
import json
from decimal import Decimal
from sqlite3 import connect

from rule_engine import Rule, ast, types

from ..exceptions import DoesNotExist, ConditionCheckFailed


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
    if isinstance(expr, ast.ArithmeticComparisonExpression):
        left, l_params = expression_to_condition(expr.left, key_name)
        right, r_params = expression_to_condition(expr.right, key_name)
        op = dict(lt="<", gt=">", lte="<=", gte=">=")[expr.type]
        return f"{left} {op} {right}", l_params + r_params
    if isinstance(expr, ast.ContainsExpression):
        container, container_params = expression_to_condition(expr.container, key_name)
        member, member_params = expression_to_condition(expr.member, key_name)

        clean_member_params = tuple(['%"' + member_params[0].strip('\"') + '"%'])
        return f"{container} like {member}", container_params + clean_member_params

    if isinstance(expr, ast.SymbolExpression):
        if expr.name == 'null':
            return None, ()
        return expr.name, ()
    if isinstance(expr, ast.NullExpression):
        return None, ()
    if isinstance(expr, (ast.StringExpression, ast.DatetimeExpression)):
        return "?", tuple([expr.value])
    if isinstance(expr, ast.FloatExpression):
        val = expr.value
        return "?", tuple([val if not types.is_integer_number(val) else int(val)])
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


def smart_serialize_array(data):
    return json.dumps(list(data))


SERIALIZE_MAP = {
    'int': int,
    'integer': int,
    'number': float,
    'decimal': str,
    'double': str,
    'string': str,
    'bool': lambda b: 1 if b else 0,
    'object': json.dumps,
    'array': smart_serialize_array,
    'anyOf': str,  # FIXME - this could be more complicated. This is a hacky fix.
}


def do_nothing(x):
    return x


DESERIALIZE_MAP = {
    'int': do_nothing,
    'integer': do_nothing,
    'number': float,
    'decimal': Decimal,
    'double': Decimal,
    'string': do_nothing,
    'bool': bool,
    'object': json.loads,
    'array': json.loads,
    'anyOf': do_nothing,  # FIXME - this could be more complicated. This is a hacky fix.
}


class Backend:
    def __init__(self, cls):
        cfg = cls.Config
        self.schema = cls.schema()
        self.hash_key = cfg.hash_key
        self.table_name = cls.get_table_name()
        self._conn = connect(cfg.database, isolation_level=None)

    def sql_field_defs(self):
        schema = self.schema
        return {
            k: SQLITE_TYPE_MAP.get(v.get('type', 'anyOf'), 'TEXT')
            for k, v in schema['properties'].items()
        }

    def initialize(self):
        field_defs = self.sql_field_defs()
        field_defs[self.hash_key] += ' PRIMARY KEY'
        fields = ', '.join(f"{k} {v}" for k, v in field_defs.items())
        c = self._conn.cursor()
        c.execute(f'''CREATE TABLE IF NOT EXISTS {self.table_name} ({fields})''')
        c.close()

    def exists(self):
        c = self._conn.cursor()
        c.execute("select sql from sqlite_master where type = 'table' and name = ?;", [self.table_name])
        res = bool(c.fetchone())
        c.close()
        return res

    def query(self, expression):
        c = self._conn.cursor()
        expression, params = rule_to_sqlite_expression(expression)
        c.execute(f"select * from {self.table_name} where {expression};", params)
        res = list(c.fetchall())
        c.close()
        schema = self.schema['properties']
        fields = list(schema.keys())
        return [
            {k: DESERIALIZE_MAP[schema[k]['type']](v) for k, v in zip(fields, rec)}
            for rec in res
        ]

    def get(self, item_key):
        c = self._conn.cursor()
        c.execute(f"select * from {self.table_name} where {self.hash_key} = ?;", [item_key])
        res = c.fetchone()
        c.close()
        if not res:
            raise DoesNotExist
        schema = self.schema['properties']
        fields = list(schema.keys())
        return {k: DESERIALIZE_MAP[schema[k].get('type', 'anyOf')](v) for k, v in zip(fields, res)}

    def save(self, item, condition: Optional[Rule] = None) -> bool:
        table_name = item.get_table_name()
        hash_key = item.Config.hash_key
        key = getattr(item, hash_key)
        fields_def = self.sql_field_defs()
        fields = tuple(fields_def.keys())

        schema = item.schema()['properties']
        item_data = item.dict()
        values = tuple([SERIALIZE_MAP[schema[field].get('type', 'anyOf')](item_data[field]) for field in fields])
        try:
            old_item = self.get(key)
            if condition and not condition.matches(old_item):
                raise ConditionCheckFailed()

            qs = ', '.join(f"{field} = ?" for field in fields)
            if condition:
                condition_expr, condition_params = rule_to_sqlite_expression(condition)
            else:
                condition_expr = f"{hash_key} = ?"
                condition_params = tuple([key])

            self._conn.execute(f"UPDATE {table_name} SET {qs} WHERE {condition_expr};", values + condition_params)
            return True
        except DoesNotExist:
            qs = ','.join(['?'] * len(fields))
            self._conn.execute(f"insert into {table_name} values ({qs})", values)
        return True

    def delete(self, item_key: str):
        self._conn.execute(f"DELETE FROM {self.table_name} WHERE {self.hash_key} = ?;", [item_key])
