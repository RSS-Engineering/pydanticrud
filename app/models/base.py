from typing import Optional
from uuid import uuid4, UUID

import boto3
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError
from pydantic import BaseModel

from ..config import AWS_REGION, AWS_DYNAMO_ENDPOINT
from ..cascade_types import FLAG_VALUE_TYPE
from ..exceptions import DoesNotExist, RevisionMismatch

dynamodb = boto3.resource(
    'dynamodb',
    region_name=AWS_REGION,
    endpoint_url=AWS_DYNAMO_ENDPOINT
)

# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/customizations/dynamodb.html#valid-dynamodb-types
DYNAMO_TYPE_MAP = {
    'int': 'N',
    'decimal': 'N',
    'double': 'N',
    'bool': 'BOOL',
}


def build_update_condition(hash_key: str, key_value: FLAG_VALUE_TYPE):
    return Key(hash_key).eq(key_value)


def build_update_condition_with_revision(
        hash_key: str,
        key_value: FLAG_VALUE_TYPE,
        revision: UUID,
        field: Optional[str] = None,
        value: Optional[FLAG_VALUE_TYPE] = None,
        allow_create: bool = False):
    condition = build_update_condition(hash_key, key_value)

    attr = Attr('revision')
    if isinstance(revision, UUID):
        condition = condition and attr.eq(str(revision))
        if None not in (field, value):
            condition = condition and Attr(field).ne(value)
    elif allow_create:
        condition = condition and attr.not_exists()
    else:
        condition = False
    return condition


class UnversionedBaseModel(BaseModel):
    @classmethod
    def exists(cls):
        table = cls.get_table()
        try:
            return table.table_status == 'ACTIVE'
        except ClientError:
            return False

    @classmethod
    def query(cls, expression):
        table = cls.get_table()
        res = table.scan(
            FilterExpression=expression
        )

        return [cls.parse_obj(i) for i in res['Items']]


    @classmethod
    def create_table(cls, wait=False) -> dynamodb.Table:
        schema = cls.schema()
        hash_key = cls.Config.hash_key
        table = dynamodb.create_table(
            AttributeDefinitions=[
                {
                    'AttributeName': hash_key,
                    'AttributeType': DYNAMO_TYPE_MAP.get(schema['properties'][hash_key]['type'], 'S')
                },
            ],
            TableName=cls.Config.title.lower(),
            KeySchema=[
                {
                    'AttributeName': hash_key,
                    'KeyType': 'HASH'
                },
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': cls.Config.read,
                'WriteCapacityUnits': cls.Config.write
            },
        )
        if wait:
            table.wait_until_exists()
        return table

    @classmethod
    def get_table_name(cls) -> str:
        return cls.Config.title.lower()

    @classmethod
    def get_table(cls) -> dynamodb.Table:
        return dynamodb.Table(cls.get_table_name())

    @classmethod
    def get(cls, item_key: str):
        resp = cls.get_table().get_item(
            Key={
                cls.Config.hash_key: item_key
            }
        )

        if 'Item' not in resp:
            raise DoesNotExist(f'{cls.Config.title} "{item_key}" does not exist')

        return cls.parse_obj(resp['Item'])

    def save(self) -> bool:
        hash_key = self.Config.hash_key
        key = getattr(self, hash_key)
        data = self.dict()

        try:
            self.get(key)  # Just to check if it exists
            condition = build_update_condition(hash_key, key)
            self.get_table().put_item(Item=data, ConditionExpression=condition)
            return False
        except DoesNotExist:
            self.get_table().put_item(Item=data)
            return True


class VersionedBaseModel(UnversionedBaseModel):
    revision: Optional[UUID]

    @classmethod
    def update_value(cls, key: str, field: str, value: FLAG_VALUE_TYPE, revision: Optional[UUID]) -> UUID:
        new_revision = uuid4()
        hash_key = cls.Config.hash_key

        condition = build_update_condition_with_revision(hash_key, key, revision, field, value)

        try:
            res = cls.get_table().update_item(
                Key={
                    hash_key: key
                },
                UpdateExpression=f"SET revision = :nr, #f = :v",
                ExpressionAttributeNames={
                    '#f': field,
                },
                ExpressionAttributeValues={
                    ':nr': str(new_revision),
                    ':v': value
                },
                ConditionExpression=condition
            )
            if res['ResponseMetadata']['HTTPStatusCode'] == 200:
                return new_revision
        except ClientError as e:
            if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                item = cls.get(key)
                if getattr(item, field) == value:
                    return revision
                raise RevisionMismatch(
                    'Provided revision is out of date' if revision else 'Must provide a revision')
            raise e

    def save(self) -> bool:
        hash_key = self.Config.hash_key
        data = self.dict()
        old_revision = data.pop('revision')
        new_revision = uuid4()
        data['revision'] = str(new_revision)

        condition = build_update_condition_with_revision(
            hash_key,
            getattr(self, hash_key),
            old_revision,
            allow_create=True
        )

        try:
            res = self.get_table().put_item(Item=data, ConditionExpression=condition)
            if res['ResponseMetadata']['HTTPStatusCode'] == 200:
                self.revision = new_revision
            return old_revision is None  # If the old_revision is None and the PUT succeeds, then it was a new object.
        except ClientError as e:
            if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                raise RevisionMismatch('Provided revision is out of date' if old_revision else 'Must provide a revision')
            raise e
