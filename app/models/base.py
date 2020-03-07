from typing import Optional
from uuid import uuid4

import boto3
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError
from pydantic import BaseModel

from app.config import AWS_REGION, AWS_DYNAMO_ENDPOINT
from app.exceptions import DoesNotExist, RevisionMismatch

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


class DynamoBaseModel(BaseModel):
    revision: Optional[str]

    @classmethod
    def exists(cls):
        table = cls.get_table()
        try:
            return table.table_status == 'ACTIVE'
        except ClientError:
            return False

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
    def get_table(cls) -> dynamodb.Table:
        return dynamodb.Table(cls.Config.title.lower())

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
        data = self.dict()
        old_revision = data.pop('revision')
        new_revision = data['revision'] = str(uuid4())

        condition = Key(hash_key).eq(getattr(self, hash_key))
        if isinstance(old_revision, str):
            condition = condition and Attr('revision').eq(old_revision)
        else:
            condition = condition and Attr('revision').not_exists()

        try:
            res = self.get_table().put_item(Item=data, ConditionExpression=condition)
            if res['ResponseMetadata']['HTTPStatusCode'] == 200:
                self.revision = new_revision
        except ClientError as e:
            if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                raise RevisionMismatch('Provided revision is out of date' if old_revision else 'Must provide a revision')
            raise e
