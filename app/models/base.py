import boto3
from botocore.exceptions import ClientError
from pydantic import BaseModel

from app.config import AWS_REGION, AWS_DYNAMO_ENDPOINT
from app.exceptions import DoesNotExist

dynamodb = boto3.resource(
    'dynamodb',
    region_name=AWS_REGION,
    endpoint_url=AWS_DYNAMO_ENDPOINT
)
DYNAMO_TYPE_MAP = {
    'int': 'N',
    'float': 'N',
    'double': 'N',
    'uuid': 'B'
}


class DynamoBaseModel(BaseModel):
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
        res = self.get_table().put_item(
            Item=self.dict()
        )

        try:
            return res['ResponseMetadata']['HTTPStatusCode'] == 200
        except KeyError:
            return False
