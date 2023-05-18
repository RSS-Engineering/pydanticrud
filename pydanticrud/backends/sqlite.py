from typing import Optional, Generic, get_type_hints
import json
from decimal import Decimal
from sqlite3 import connect, PARSE_DECLTYPES, register_converter, register_adapter

try:
    from dataclass import dataclass
except ImportError:
    from dataclasses import dataclass

from rule_engine import Rule, ast, types

from ..exceptions import DoesNotExist, ConditionCheckFailed


# SQLite can store any type of serializable data and Python is pretty good about serializing data, but only some types
# can maintain query-ability and can be included in conditions.
SQLITE_NATIVE_TYPES = {"int", "float", "bool", "str", "datetime"}

# We can add support for other data-types by specifying how they should be (de)serialized.
ADAPTERS_CONVERTERS = {
    Decimal: (str, lambda x: Decimal(x.decode())),
}


@dataclass
class ColumnMetaData:
    python_type: type
    python_type_name: str
    sqlite_native: bool


def get_column_data(field_type):
    if hasattr(field_type, "__origin__"):
        field_type = getattr(field_type, "__origin__")
    python_type_name = field_type.__name__
    sqlite_native = python_type_name.lower() in SQLITE_NATIVE_TYPES

    return ColumnMetaData(
        python_type=field_type,
        python_type_name=python_type_name,
        sqlite_native=sqlite_native,
    )


class Backend:
    def __init__(self, cls):
        cfg = cls.Config
        self.hash_key = cfg.hash_key
        self.table_name = cls.get_table_name()

        type_hints = get_type_hints(cls)
        self._columns = tuple(type_hints.keys())
        self._columns = {
            field_name: get_column_data(field_type) for field_name, field_type in type_hints.items()
            if not field_name.startswith('__')
        }
        _non_native_column_types = set(
            col.python_type for col in self._columns.values() if not col.sqlite_native
        )
        for python_type in _non_native_column_types:
            adapter, converter = ADAPTERS_CONVERTERS.get(python_type, (json.dumps, json.loads))
            register_adapter(python_type, adapter)
            register_converter(python_type.__name__, converter)

        self._conn = connect(cfg.database, detect_types=PARSE_DECLTYPES)

    def _deserialize_record(self, res_tuple) -> dict:
        """
        Match values with their field names into a dict
        """
        return {
            field_name: value for value, (field_name, f) in zip(res_tuple, self._columns.items())
        }

    def _expression_to_condition(self, expr, key_name: Optional[str] = None):
        if isinstance(expr, ast.LogicExpression):
            left, l_params = self._expression_to_condition(expr.left, key_name)
            right, r_params = self._expression_to_condition(expr.right, key_name)
            op = expr.type.upper()
            return f"({left} {op} {right})", l_params + r_params

        if isinstance(expr, ast.ComparisonExpression):
            left, l_params = self._expression_to_condition(expr.left, key_name)
            right, r_params = self._expression_to_condition(expr.right, key_name)
            op = dict(eq="=", ne="!=", lt="<", gt=">")[expr.type]
            if right is None:
                op = dict(eq="IS", ne="IS NOT")[expr.type]
                right = "NULL"
            return f"{left} {op} {right}", l_params + r_params

        if isinstance(expr, ast.ArithmeticComparisonExpression):
            left, l_params = self._expression_to_condition(expr.left, key_name)
            right, r_params = self._expression_to_condition(expr.right, key_name)
            op = dict(lt="<", gt=">", lte="<=", gte=">=")[expr.type]
            return f"{left} {op} {right}", l_params + r_params

        if isinstance(expr, ast.ContainsExpression):
            container, container_params = self._expression_to_condition(expr.container, key_name)
            member, member_params = self._expression_to_condition(expr.member, key_name)
            clean_member_params = tuple(["%" + member_params[0].strip('"') + "%"])
            return f"{container} like {member}", container_params + clean_member_params

        if isinstance(expr, ast.SymbolExpression):
            if expr.name == "null":
                return None, ()
            field_name = expr.name
            if field_name not in self._columns:
                raise SyntaxError(f"Cannot query on non-existent field: {field_name}")
            if not self._columns[field_name].sqlite_native:
                raise SyntaxError(f"Cannot query on non-native field: {field_name}")
            return expr.name, ()

        if isinstance(expr, ast.NullExpression):
            return None, ()

        if isinstance(expr, (ast.StringExpression, ast.DatetimeExpression)):
            return "?", tuple([expr.value])

        if isinstance(expr, ast.FloatExpression):
            val = expr.value
            return "?", tuple([val if not types.is_integer_number(val) else int(val)])

        raise NotImplementedError

    def _rule_to_sqlite_expression(self, rule: Rule, key_name: Optional[str] = None):
        return self._expression_to_condition(rule.statement.expression, key_name)

    def initialize(self):
        field_defs = {
            field_name: f.python_type_name.upper() for field_name, f in self._columns.items()
        }
        field_defs[self.hash_key] += " PRIMARY KEY"
        fields = ", ".join(f"{k} {v}" for k, v in field_defs.items())
        self._conn.execute(f"CREATE TABLE IF NOT EXISTS {self.table_name} ({fields})")

    def exists(self) -> bool:
        c = self._conn.execute(
            "select sql from sqlite_master where type = 'table' and name = ?;", [self.table_name]
        )
        res = bool(c.fetchone())
        return res

    def query(self, expression) -> list:
        expression, params = self._rule_to_sqlite_expression(expression)
        return [
            self._deserialize_record(rec)
            for rec in self._conn.execute(
                f"select * from {self.table_name} where {expression};", params
            )
        ]

    def get(self, item_key):
        c = self._conn.execute(
            f"select * from {self.table_name} where {self.hash_key} = ?;", [item_key]
        )
        res = c.fetchone()
        if not res:
            raise DoesNotExist
        return self._deserialize_record(res)

    def save(self, item, condition: Optional[Rule] = None) -> bool:
        table_name = item.get_table_name()
        hash_key = item.Config.hash_key
        key = getattr(item, hash_key)
        fields = tuple(self._columns.keys())

        item_data = item.dict()
        values = tuple([item_data[field] for field in fields])
        try:
            old_item = self.get(key)
            if condition and not condition.matches(old_item):
                raise ConditionCheckFailed()

            qs = ", ".join(f"{field} = ?" for field in fields)
            if condition:
                condition_expr, condition_params = self._rule_to_sqlite_expression(condition)
            else:
                condition_expr = f"{hash_key} = ?"
                condition_params = tuple([key])
            self._conn.execute(
                f"UPDATE {table_name} SET {qs} WHERE {condition_expr};",
                values + condition_params,
            )
            return True
        except DoesNotExist:
            qs = ",".join(["?"] * len(fields))
            self._conn.execute(f"insert into {table_name} values ({qs})", values)
        return True

    def delete(self, item_key: str):
        self._conn.execute(f"DELETE FROM {self.table_name} WHERE {self.hash_key} = ?;", [item_key])
