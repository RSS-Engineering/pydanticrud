from typing import Optional
from decimal import Decimal
import json

import boto3
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError
from rule_engine import Rule, ast, types

from ..exceptions import DoesNotExist, ConditionCheckFailed


def expression_to_condition(expr, key_name: Optional[str] = None):
    if isinstance(expr, ast.LogicExpression):
        left = expression_to_condition(expr.left, key_name)
        right = expression_to_condition(expr.right, key_name)
        if expr.type == "and":
            return left and right
        if expr.type == "or":
            return left or right
    if isinstance(expr, ast.ComparisonExpression):
        left = expression_to_condition(expr.left, key_name)
        right = expression_to_condition(expr.right, key_name)
        if expr.type == "eq":
            return left.eq(right) if right is not None else left.not_exists()
        if expr.type == "ne":
            return left.ne(right) if right is not None else left.exists()
    if isinstance(expr, ast.ArithmeticComparisonExpression):
        left, l_params = expression_to_condition(expr.left, key_name)
        right, r_params = expression_to_condition(expr.right, key_name)
        return getattr(left, expr.type)(right)
    if isinstance(expr, ast.SymbolExpression):
        if key_name is not None and expr.name == key_name:
            return Key(expr.name)
        return Attr(expr.name)
    if isinstance(expr, ast.NullExpression):
        return None
    if isinstance(expr, (ast.StringExpression, ast.DatetimeExpression)):
        return expr.value
    if isinstance(expr, ast.FloatExpression):
        val = expr.value
        return "?", tuple([val if not types.is_integer_number(val) else int(val)])
    if isinstance(expr, ast.ContainsExpression):
        container = expression_to_condition(expr.container, key_name)
        member = expression_to_condition(expr.member, key_name)
        return container.contains(member)
    raise NotImplementedError


def rule_to_boto_expression(rule: Rule, key_name: Optional[str] = None):
    return expression_to_condition(rule.statement.expression, key_name)


# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/customizations/dynamodb.html#valid-dynamodb-types
DYNAMO_TYPE_MAP = {
    "int": "N",
    "decimal": "N",
    "double": "N",
    "bool": "BOOL",
}

SERIALIZE_MAP = {
    "int": int,
    "integer": int,
    "number": Decimal,
    "decimal": str,
    "double": str,
    "string": str,
    "object": json.dumps,
    "anyOf": str,  # FIXME - this could be more complicated. This is a hacky fix.
}


def do_nothing(x):
    return x


DESERIALIZE_MAP = {
    "int": do_nothing,
    "integer": do_nothing,
    "number": float,
    "decimal": Decimal,
    "double": Decimal,
    "string": do_nothing,
    "bool": bool,
    "object": json.loads,
    "array": json.loads,
    "anyOf": do_nothing,  # FIXME - this could be more complicated. This is a hacky fix.
}


class Backend:
    def __init__(self, cls):
        cfg = cls.Config
        self.schema = cls.schema()
        self.hash_key = cfg.hash_key
        self.table_name = cls.get_table_name()
        self.dynamodb = boto3.resource(
            "dynamodb",
            region_name=getattr(cfg, "region", "us-east-2"),
            endpoint_url=getattr(cfg, "endpoint", None),
        )

    def _serialize_record(self, data_dict) -> dict:
        """
        Apply converters to non-native types
        """
        schema = self.schema["properties"]
        return {
            field_name: SERIALIZE_MAP[schema[field_name].get("type", "anyOf")](value)
            for field_name, value in data_dict.items()
        }

    def _deserialize_record(self, data_dict) -> dict:
        """
        Apply converters to non-native types
        """
        schema = self.schema["properties"]
        return {
            field_name: DESERIALIZE_MAP[schema[field_name]["type"]](value)
            for field_name, value in data_dict.items()
        }

    def initialize(self):
        schema = self.schema
        hash_key = self.hash_key

        table = self.dynamodb.create_table(
            AttributeDefinitions=[
                {
                    "AttributeName": hash_key,
                    "AttributeType": DYNAMO_TYPE_MAP.get(
                        schema["properties"][hash_key].get("type", "anyOf"), "S"
                    ),
                },
            ],
            TableName=self.table_name,
            KeySchema=[
                {"AttributeName": hash_key, "KeyType": "HASH"},
            ],
            ProvisionedThroughput={"ReadCapacityUnits": 1, "WriteCapacityUnits": 1},
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

    def query(self, expression):
        table = self.get_table()
        resp = table.scan(FilterExpression=rule_to_boto_expression(expression, self.hash_key))
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
