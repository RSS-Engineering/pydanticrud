from typing import Optional

import boto3
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError
from pydantic import BaseSettings
from rule_engine import Rule, ast

from ..exceptions import DoesNotExist, ConditionCheckFailed


class Settings(BaseSettings):
    ACCESS_KEY_ID: str
    SECRET_ACCESS_KEY: str
    REGION: str = 'us-east-2'
    ENDPOINT: Optional[str] = None
    INITIALIZE: bool = False

    class Config:
        env_prefix = 'DYNAMO_'
        fields = {
            "ACCESS_KEY_ID": {
                "env": "AWS_ACCESS_KEY_ID"
            },
            "SECRET_ACCESS_KEY": {
                "env": "AWS_SECRET_ACCESS_KEY"
            },
            "REGION": {
                "env": ["DYNAMO_REGION", "AWS_REGION"]
            }
        }


def expression_to_condition(expr, key_name: Optional[str] = None):
    if isinstance(expr, ast.LogicExpression):
        left = expression_to_condition(expr.left, key_name)
        right = expression_to_condition(expr.right, key_name)
        if expr.type == 'and':
            return left and right
        if expr.type == 'or':
            return left or right
    if isinstance(expr, ast.ComparisonExpression):
        left = expression_to_condition(expr.left, key_name)
        right = expression_to_condition(expr.right, key_name)
        if expr.type == 'eq':
            return left.eq(right) if right is not None else left.not_exists()
        if expr.type == 'ne':
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
    if isinstance(expr, ast.ContainsExpression):
        container = expression_to_condition(expr.container, key_name)
        member = expression_to_condition(expr.member, key_name)
        return container.contains(member)
    raise NotImplementedError


def rule_to_boto_expression(rule: Rule, key_name: Optional[str] = None):
    return expression_to_condition(rule.statement.expression, key_name)


# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/customizations/dynamodb.html#valid-dynamodb-types
DYNAMO_TYPE_MAP = {
    'int': 'N',
    'decimal': 'N',
    'double': 'N',
    'bool': 'BOOL',
}


class Backend:
    def __init__(self, cls):
        self.settings = Settings()
        self.schema = cls.schema()
        self.hash_key = cls.Config.hash_key
        self.table_name = cls.get_table_name()
        self.dynamodb = boto3.resource(
            'dynamodb',
            region_name=self.settings.REGION,
            endpoint_url=self.settings.ENDPOINT
        )

    def initialize(self):
        schema = self.schema
        hash_key = self.hash_key

        table = self.dynamodb.create_table(
            AttributeDefinitions=[
                {
                    'AttributeName': hash_key,
                    'AttributeType': DYNAMO_TYPE_MAP.get(schema['properties'][hash_key]['type'], 'S')
                },
            ],
            TableName=self.table_name,
            KeySchema=[
                {
                    'AttributeName': hash_key,
                    'KeyType': 'HASH'
                },
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 1,
                'WriteCapacityUnits': 1
            },
        )
        table.wait_until_exists()

    def get_table(self):
        return self.dynamodb.Table(self.table_name)

    def exists(self):
        table = self.get_table()
        try:
            return table.table_status == 'ACTIVE'
        except ClientError:
            return False

    def query(self, expression):
        table = self.get_table()
        res = table.scan(
            FilterExpression=rule_to_boto_expression(expression, self.hash_key)
        )
        return res['Items']

    def get(self, item_key):
        resp = self.get_table().get_item(
            Key={
                self.hash_key: item_key
            }
        )

        if 'Item' not in resp:
            raise DoesNotExist(f'{self.table_name} "{item_key}" does not exist')
        return resp['Item']

    def save(self, item, condition: Optional[Rule] = None) -> bool:
        cls = item.__class__
        hash_key = self.hash_key
        data = item.dict()

        try:
            if condition:
                res = self.get_table().put_item(
                    Item=data,
                    ConditionExpression=rule_to_boto_expression(condition, hash_key)
                )
            else:
                res = self.get_table().put_item(Item=data)
            return res['ResponseMetadata']['HTTPStatusCode'] == 200

        except ClientError as e:
            if e.response['Error']['Code'] == 'ConditionalCheckFailedException' and condition:
                raise ConditionCheckFailed()
            raise e

    def delete(self, item_key: str):
        self.get_table().delete_item(
            Key={
                self.hash_key: item_key
            }
        )
