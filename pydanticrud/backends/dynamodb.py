from typing import Optional, Set
import logging
import json
from datetime import datetime

import boto3
from boto3.dynamodb.conditions import Key, Attr
from boto3.exceptions import DynamoDBNeedsKeyConditionError
from botocore.exceptions import ClientError
from rule_engine import Rule, ast, types

from ..exceptions import DoesNotExist, ConditionCheckFailed

log = logging.getLogger(__name__)


def expression_to_condition(expr, keys: set):
    if isinstance(expr, ast.LogicExpression):
        left, l_keys = expression_to_condition(expr.left, keys)
        right, r_keys = expression_to_condition(expr.right, keys)
        if expr.type == "and":
            return left & right, l_keys | r_keys
        if expr.type == "or":
            return left | right, l_keys | r_keys
    if isinstance(expr, ast.ComparisonExpression):
        left, l_keys = expression_to_condition(expr.left, keys)
        right, r_keys = expression_to_condition(expr.right, keys)
        exit_keys = l_keys | r_keys
        if expr.type == "eq":
            if right is not None:
                return left.eq(right), exit_keys
            else:
                return left.not_exists(), exit_keys
        if expr.type == "ne":
            if right is not None:
                return left.ne(right), exit_keys
            else:
                return left.exists(), exit_keys
        return getattr(left, {"le": "lte", "ge": "gte"}.get(expr.type, expr.type))(right), exit_keys
    if isinstance(expr, ast.SymbolExpression):
        if keys is not None and expr.name in keys:
            return Key(expr.name), {expr.name}
        return Attr(expr.name), set()
    if isinstance(expr, ast.NullExpression):
        return None, set()
    if isinstance(expr, ast.DatetimeExpression):
        return _to_epoch_float(expr.value), set()
    if isinstance(expr, ast.StringExpression):
        return expr.value, set()
    if isinstance(expr, ast.FloatExpression):
        val = expr.value
        return val if not types.is_integer_number(val) else int(val), set()
    if isinstance(expr, ast.ContainsExpression):
        container, l_keys = expression_to_condition(expr.container, keys)
        member, r_keys = expression_to_condition(expr.member, keys)
        return container.contains(member), l_keys | r_keys
    raise NotImplementedError


def rule_to_boto_expression(rule: Rule, keys: Optional[Set[str]] = None):
    return expression_to_condition(rule.statement.expression, keys or set())


# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/customizations/dynamodb.html#valid-dynamodb-types
DYNAMO_TYPE_MAP = {
    "integer": "N",
    "decimal": "N",
    "double": "N",
    "bool": "BOOL",
}

EPOCH = datetime.utcfromtimestamp(0)


def _to_epoch_float(dt):
    return (dt - EPOCH).total_seconds * 1000


SERIALIZE_MAP = {
    "number": str,  # float or decimal
    "string": lambda d: d.isoformat() if isinstance(d, datetime) else d,  # string, datetime
    "boolean": lambda d: 1 if d else 0,
    "object": json.dumps,
    "array": json.dumps,
    "anyOf": str,  # FIXME - this could be more complicated. This is a hacky fix.
}


def do_nothing(x):
    return x


DESERIALIZE_MAP = {
    "number": float,
    "string": do_nothing,
    "boolean": bool,
    "object": json.loads,
    "array": json.loads,
    "anyOf": do_nothing,  # FIXME - this could be more complicated. This is a hacky fix.
}


def index_definition(index_name, keys, gsi=False):
    schema = {
        "IndexName": index_name,
        "Projection": {
            "ProjectionType": "ALL",
        },
        "KeySchema": [
            {"AttributeName": attr, "KeyType": ["HASH", "RANGE"][i]}
            for i, attr in enumerate(keys)
        ],
    }
    if gsi:
        schema["ProvisionedThroughput"] = {"ReadCapacityUnits": 1, "WriteCapacityUnits": 1}
    return schema


class Backend:
    def __init__(self, cls):
        cfg = cls.Config
        self.schema = cls.schema()
        self.hash_key = cfg.hash_key
        self.range_key = getattr(cfg, 'range_key', None)
        self.table_name = cls.get_table_name()

        self.local_indexes = getattr(cfg, "local_indexes", {})
        self.global_indexes = getattr(cfg, "global_indexes", {})
        self.index_map = {(self.hash_key,): None}
        self.possible_keys = {self.hash_key}
        if self.range_key:
            self.possible_keys.add(self.range_key)
            self.index_map = {(self.hash_key, self.range_key): None}

        for name, keys in dict(**self.local_indexes, **self.global_indexes).items():
            self.index_map[keys] = name
            for key in keys:
                self.possible_keys.add(key)

        self.dynamodb = boto3.resource(
            "dynamodb",
            region_name=getattr(cfg, "region", "us-east-2"),
            endpoint_url=getattr(cfg, "endpoint", None),
        )

    def _serialize_field(self, field_name, value):
        schema = self.schema["properties"]
        field_type = schema[field_name].get("type", "anyOf")
        try:
            return SERIALIZE_MAP[field_type](value)
        except KeyError:
            log.debug(f"No serializer for field_type {field_type}")
            return value  # do nothing but log it.

    def _serialize_record(self, data_dict) -> dict:
        """
        Apply converters to non-native types
        """
        return {
            field_name: self._serialize_field(field_name, value)
            for field_name, value in data_dict.items()
        }

    def _deserialize_field(self, field_name, value):
        schema = self.schema["properties"]
        field_type = schema[field_name].get("type", "anyOf")
        try:
            return DESERIALIZE_MAP[field_type](value)
        except KeyError:
            log.debug(f"No deserializer for field_type {field_type}")
            return value  # do nothing but log it.

    def _deserialize_record(self, data_dict) -> dict:
        """
        Apply converters to non-native types
        """
        return {
            field_name: self._deserialize_field(field_name, value)
            for field_name, value in data_dict.items()
        }

    def _key_param_to_dict(self, key):
        _key = {
            self.hash_key: key,
        }
        if self.range_key:
            if not isinstance(key, tuple) or not len(key) == 2:
                raise ValueError(f"{self.table_name} needs both a hash_key and a range_key to delete a record.")
            _key = {
                self.hash_key: key[0],
                self.range_key: key[1]
            }
        return _key

    def _get_best_index(self, keys_used: Set[str]):
        def score_index(index):
            if set(index) == keys_used:
                # perfect match
                return 3
            elif len(index) > len(keys_used):
                # index match with additional filter
                return 2

            # We shouldn't get here.
            raise NotImplementedError()

        possible_indexes = sorted(
            [
                key
                for key in self.index_map.keys()
                if set(key).issubset(keys_used)
            ],
            key=score_index
        )

        if possible_indexes:
            return self.index_map[possible_indexes[0]]
        return None

    def initialize(self):
        schema = self.schema
        gsies = {k: v for k, v in self.global_indexes.items()}
        lsies = {k: v for k, v in self.local_indexes.items()}
        key_names = [key for key in [self.hash_key, self.range_key] if key]

        table_schema = dict(
            AttributeDefinitions=[
                {
                    "AttributeName": attr,
                    "AttributeType": DYNAMO_TYPE_MAP.get(
                        schema["properties"][attr].get("type", "anyOf"), "S"
                    ),
                }
                for attr in self.possible_keys
            ],
            TableName=self.table_name,
            KeySchema=[
                {"AttributeName": key, "KeyType": ["HASH", "RANGE"][i]}
                for i, key in enumerate(key_names)
            ],
            ProvisionedThroughput={"ReadCapacityUnits": 1, "WriteCapacityUnits": 1},
        )
        if lsies:
            table_schema['LocalSecondaryIndexes'] = [
                index_definition(index_name, keys)
                for index_name, keys in lsies.items()
            ]
        if gsies:
            table_schema['GlobalSecondaryIndexes'] = [
                index_definition(index_name, keys, gsi=True)
                for index_name, keys in gsies.items()
            ]
        table = self.dynamodb.create_table(**table_schema)
        table.wait_until_exists()

    def get_table(self):
        return self.dynamodb.Table(self.table_name)

    def exists(self):
        table = self.get_table()
        try:
            return table.table_status == "ACTIVE"
        except ClientError:
            return False

    def query(self, query_expr: Optional[Rule] = None, filter_expr: Optional[Rule] = None):
        table = self.get_table()
        f_expr, _ = rule_to_boto_expression(filter_expr) if filter_expr else (None, set())

        params = {}
        if f_expr:
            params["FilterExpression"] = f_expr

        if query_expr:
            q_expr, keys_used = rule_to_boto_expression(query_expr, self.possible_keys)

            if not keys_used and not filter_expr:
                raise ConditionCheckFailed("No keys in query expression. Use a filter expression or add an index.")

            index_name = self._get_best_index(keys_used)
            params["KeyConditionExpression"] = q_expr

            if index_name:
                params["IndexName"] = index_name
            elif not keys_used.issubset({self.hash_key, self.range_key}):
                raise ConditionCheckFailed("No keys in expression. Enable scan or add an index.")

            try:
                resp = table.query(**params)
            except DynamoDBNeedsKeyConditionError:
                raise ConditionCheckFailed("Non-key attributes are not valid in the query expression. Use filter "
                                           "expression")

        else:
            resp = table.scan(**params)

        return [self._deserialize_record(rec) for rec in resp["Items"]]

    def get(self, key):
        _key = self._key_param_to_dict(key)
        resp = self.get_table().get_item(Key=_key)

        if "Item" not in resp:
            if not self.range_key:
                _key = key
            raise DoesNotExist(f'{self.table_name} "{_key}" does not exist')

        return self._deserialize_record(resp["Item"])

    def save(self, item, condition: Optional[Rule] = None) -> bool:
        data = self._serialize_record(item.dict())

        try:
            if condition:
                expr, _ = rule_to_boto_expression(condition, self.possible_keys)
                res = self.get_table().put_item(
                    Item=data,
                    ConditionExpression=expr,
                )
            else:
                res = self.get_table().put_item(Item=data)
            return res["ResponseMetadata"]["HTTPStatusCode"] == 200

        except ClientError as e:
            if e.response["Error"]["Code"] == "ConditionalCheckFailedException" and condition:
                raise ConditionCheckFailed()
            raise e

    def delete(self, key):
        self.get_table().delete_item(Key=self._key_param_to_dict(key))
