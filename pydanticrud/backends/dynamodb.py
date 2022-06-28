from typing import Optional
import logging
import json
from datetime import datetime

import boto3
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError
from rule_engine import Rule, ast, types

from ..exceptions import DoesNotExist, ConditionCheckFailed

log = logging.getLogger(__name__)


def expression_to_condition(expr, keys: set):
    if isinstance(expr, ast.LogicExpression):
        left, l_keys = expression_to_condition(expr.left, keys)
        right, r_keys = expression_to_condition(expr.right, keys)
        if expr.type == "and":
            return left and right, l_keys | r_keys
        if expr.type == "or":
            return left or right, l_keys | r_keys
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


def rule_to_boto_expression(rule: Rule, keys: set):
    return expression_to_condition(rule.statement.expression, keys)


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


class Backend:
    def __init__(self, cls):
        cfg = cls.Config
        self.schema = cls.schema()
        self.hash_key = cfg.hash_key
        self.range_key = getattr(cfg, 'range_key', None)
        self.table_name = cls.get_table_name()

        self.indexes = getattr(cfg, "indexes", {})
        self.index_map = {(self.hash_key,): None}
        self.possible_keys = {self.hash_key}
        if self.range_key:
            self.possible_keys.add(self.range_key)
            self.index_map = {(self.hash_key, self.range_key): None}

        for name, keys in self.indexes.items():
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

    def initialize(self):
        schema = self.schema
        indexes = {k: v for k, v in self.indexes.items() if k}
        key_names = {key for key in [self.hash_key, self.range_key] if key}

        table = self.dynamodb.create_table(
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
            GlobalSecondaryIndexes=[
                {
                    "IndexName": index_name,
                    "Projection": {
                        "ProjectionType": "ALL",
                    },
                    "ProvisionedThroughput": {"ReadCapacityUnits": 1, "WriteCapacityUnits": 1},
                    "KeySchema": [
                        {"AttributeName": attr, "KeyType": ["HASH", "RANGE"][i]}
                        for i, attr in enumerate(keys)
                    ],
                }
                for index_name, keys in indexes.items()
            ],
        )
        table.wait_until_exists()

    def get_table(self):
        return self.dynamodb.Table(self.table_name)

    def exists(self):
        table = self.get_table()
        try:
            return table.table_status == "ACTIVE"
        except ClientError:
            return False

    def query(self, expression, scan=False):
        table = self.get_table()
        expr, keys_used = rule_to_boto_expression(expression, self.possible_keys)
        if not keys_used and not scan:
            raise ConditionCheckFailed("No keys in expression. Enable scan or add an index.")
        if not scan:
            index_name = self.index_map.get(tuple(keys_used))
            params = dict(KeyConditionExpression=expr)
            if index_name:
                params["IndexName"] = index_name
            resp = table.query(**params)
        else:
            resp = table.scan(FilterExpression=expr)
        return [self._deserialize_record(rec) for rec in resp["Items"]]

    def get(self, item_key):
        resp = self.get_table().get_item(Key={self.hash_key: item_key})

        if "Item" not in resp:
            raise DoesNotExist(f'{self.table_name} "{item_key}" does not exist')
        return self._deserialize_record(resp["Item"])

    def save(self, item, condition: Optional[Rule] = None) -> bool:
        hash_key = self.hash_key
        data = self._serialize_record(item.dict())

        try:
            if condition:
                res = self.get_table().put_item(
                    Item=data,
                    ConditionExpression=rule_to_boto_expression(condition, hash_key),
                )
            else:
                res = self.get_table().put_item(Item=data)
            return res["ResponseMetadata"]["HTTPStatusCode"] == 200

        except ClientError as e:
            if e.response["Error"]["Code"] == "ConditionalCheckFailedException" and condition:
                raise ConditionCheckFailed()
            raise e

    def delete(self, item_key: str):
        self.get_table().delete_item(Key={self.hash_key: item_key})
