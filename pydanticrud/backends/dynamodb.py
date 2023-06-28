from typing import Optional, Set, Union, Dict, Any
import logging
import json
from datetime import datetime, timezone
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Key, Attr
from boto3.exceptions import DynamoDBNeedsKeyConditionError
from botocore.exceptions import ClientError
from rule_engine import Rule, ast, types

from ..main import IterableResult
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
        return _to_epoch_decimal(expr.value), set()
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

EPOCH = datetime(1970, 1, 1, 0, 0)


def _to_epoch_decimal(dt: datetime) -> Decimal:
    """TTL fields must be stored as a float but boto only supports decimals."""
    epock = EPOCH
    if dt.tzinfo:
        epock = epock.replace(tzinfo=timezone.utc)
    return Decimal((dt - epock).total_seconds())


SERIALIZE_MAP = {
    "number": str,  # float or decimal
    "string": str,
    "string:date-time": lambda d: d.isoformat(),
    "string:ttl": lambda d: _to_epoch_decimal(d),
    "boolean": lambda d: 1 if d else 0,
    "object": json.dumps,
    "array": json.dumps,
}

DESERIALIZE_MAP = {
    "number": float,
    "boolean": bool,
    "object": json.loads,
    "array": json.loads,
}


def chunk_list(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i : i + size]


def index_definition(index_name, keys, gsi=False):
    schema = {
        "IndexName": index_name,
        "Projection": {
            "ProjectionType": "ALL",
        },
        "KeySchema": [
            {"AttributeName": attr, "KeyType": ["HASH", "RANGE"][i]} for i, attr in enumerate(keys)
        ],
    }
    if gsi:
        schema["ProvisionedThroughput"] = {"ReadCapacityUnits": 1, "WriteCapacityUnits": 1}
    return schema


class DynamoSerializer:
    def __init__(self, schema, ttl_field=None):
        self.properties = schema.get("properties")
        self.definitions = schema.get("definitions")
        self.ttl_field = ttl_field

    def _get_type_possibilities(self, field_name) -> Set[tuple]:
        field_properties = self.properties.get(field_name)

        if not field_properties:
            return set()

        possible_types = []
        if "anyOf" in field_properties:
            possible_types.extend([r.get("$ref", r) for r in field_properties["anyOf"]])
        else:
            possible_types.append(field_properties.get("$ref", field_properties))

        def type_from_definition(definition_signature: Union[str, dict]) -> dict:
            if isinstance(definition_signature, str):
                t = definition_signature.split("/")[-1]
                return self.definitions[t]
            return definition_signature

        type_dicts = [type_from_definition(t) for t in possible_types]

        return set([(t["type"], t.get("format", "")) for t in type_dicts])

    def _serialize_field(self, field_name, value):
        if field_name == self.ttl_field:
            field_types = {("string", "ttl")}
        else:
            field_types = self._get_type_possibilities(field_name)

        if value is not None:
            for t in field_types:
                try:
                    type_signature = ":".join(t).rstrip(":")
                    try:
                        return SERIALIZE_MAP[type_signature](value)
                    except KeyError:
                        return SERIALIZE_MAP[t[0]](value)
                except (ValueError, TypeError, KeyError):
                    pass

        # If we got a value that is not part of the schema, pass it
        # through and let pydantic sort it out.
        return value

    def serialize_record(self, data_dict) -> dict:
        """
        Apply converters to non-native types
        """
        return {
            field_name: self._serialize_field(field_name, value)
            for field_name, value in data_dict.items()
        }

    def _deserialize_field(self, field_name, value):
        field_types = self._get_type_possibilities(field_name)
        if value is not None:
            for t in field_types:
                try:
                    type_signature = ":".join(t).rstrip(":")
                    try:
                        return DESERIALIZE_MAP[type_signature](value)
                    except KeyError:
                        return DESERIALIZE_MAP[t[0]](value)
                except (ValueError, TypeError, KeyError):
                    pass

        return value

    def deserialize_record(self, data_dict) -> dict:
        """
        Apply converters to non-native types
        """
        return {
            field_name: self._deserialize_field(field_name, value)
            for field_name, value in data_dict.items()
        }


class DynamoIterableResult(IterableResult):
    def __init__(self, cls, result, serialized_items):
        super(DynamoIterableResult, self).__init__(cls, serialized_items, result.get("Count"))

        self.last_evaluated_key = result.get("LastEvaluatedKey")
        self.scanned_count = result["ScannedCount"]


class Backend:
    def __init__(self, cls):
        cfg = cls.Config
        self.cls = cls
        self.schema = cls.schema()
        self.hash_key = cfg.hash_key
        self.range_key = getattr(cfg, "range_key", None)
        self.serializer = DynamoSerializer(self.schema, ttl_field=getattr(cfg, "ttl", None))
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

    def _key_param_to_dict(self, key):
        _key = {
            self.hash_key: key,
        }
        if self.range_key:
            if not isinstance(key, tuple) or not len(key) == 2:
                raise ValueError(f"{self.table_name} needs both a hash_key and a range_key.")
            _key = {self.hash_key: key[0], self.range_key: key[1]}
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
            [key for key in self.index_map.keys() if set(key).issubset(keys_used)], key=score_index
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
                        schema["properties"][attr].get("type"), "S"
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
            table_schema["LocalSecondaryIndexes"] = [
                index_definition(index_name, keys) for index_name, keys in lsies.items()
            ]
        if gsies:
            table_schema["GlobalSecondaryIndexes"] = [
                index_definition(index_name, keys, gsi=True) for index_name, keys in gsies.items()
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

    def query(
        self,
        query_expr: Optional[Rule] = None,
        filter_expr: Optional[Rule] = None,
        limit: Optional[int] = None,
        exclusive_start_key: Optional[str] = None,
        order: str = "asc",
    ):
        table = self.get_table()
        f_expr, _ = rule_to_boto_expression(filter_expr) if filter_expr else (None, set())

        params = {}

        if limit:
            params["Limit"] = limit
        if exclusive_start_key:
            params["ExclusiveStartKey"] = exclusive_start_key
        if f_expr:
            params["FilterExpression"] = f_expr

        if query_expr:
            q_expr, keys_used = rule_to_boto_expression(query_expr, self.possible_keys)

            if not keys_used and not filter_expr:
                raise ConditionCheckFailed(
                    "No keys in query expression. Use a filter expression or add an index."
                )

            index_name = self._get_best_index(keys_used)
            params["KeyConditionExpression"] = q_expr

            if order != "asc":
                params["ScanIndexForward"] = False

            if index_name:
                params["IndexName"] = index_name
            elif not keys_used.issubset({self.hash_key, self.range_key}):
                raise ConditionCheckFailed("No keys in expression. Enable scan or add an index.")

            try:
                resp = table.query(**params)
            except ClientError as e:
                if e.response["Error"]["Code"] == "ResourceNotFoundException":
                    return []
                raise e
            except DynamoDBNeedsKeyConditionError:
                raise ConditionCheckFailed(
                    "Non-key attributes are not valid in the query expression. Use filter "
                    "expression"
                )
        else:
            if order != "asc":
                raise ConditionCheckFailed("Scans do not support reverse order.")

            try:
                resp = table.scan(**params)
            except ClientError as e:
                if e.response["Error"]["Code"] == "ResourceNotFoundException":
                    return []
                raise e

        return DynamoIterableResult(
            self.cls, resp, (self.serializer.deserialize_record(rec) for rec in resp["Items"])
        )

    def get(self, key: Union[Dict, Any]):
        if isinstance(key, dict):
            try:
                return self.query(
                    Rule(" and ".join(f"{k} == {repr(v)}" for k, v in key.items())), limit=1
                )[0]
            except IndexError:
                raise DoesNotExist(f'{self.table_name} "{key}" does not exist')

        _key: Dict[str, str] = self._key_param_to_dict(key)
        try:
            resp = self.get_table().get_item(Key=_key)
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceNotFoundException":
                raise DoesNotExist(f'{self.table_name} "{_key}" does not exist')
            raise e

        if "Item" not in resp:
            if not self.range_key:
                _key = key
            raise DoesNotExist(f'{self.table_name} "{_key}" does not exist')

        return self.serializer.deserialize_record(resp["Item"])

    def save(self, item, condition: Optional[Rule] = None) -> bool:
        data = self.serializer.serialize_record(item.dict(by_alias=True))

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

    def batch_save(self, items: list) -> dict:
        """
        This function is to write multiple records in to dynamodb and returns unprocessed records in dict
        if something gone wrong with the record.Currently, batch_write is not supporting ConditionExpression
        Refer docs:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/batch_write_item.html
        """
        # Prepare the batch write requests
        request_items = {self.table_name: []}

        # chunk list for size limit of 25 items to write using this batch_write operation refer below.
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/batch_write_item.html#:~:text=The%20BatchWriteItem%20operation,Data%20Types.
        for chunk in chunk_list(items, 25):
            serialized_items = [
                self.serializer.serialize_record(item.dict(by_alias=True)) for item in chunk
            ]
            for serialized_item in serialized_items:
                request_items[self.table_name].append({"PutRequest": {"Item": serialized_item}})
        try:
            response = self.dynamodb.batch_write_item(RequestItems=request_items)
        except ClientError as e:
            raise e
        except (ValueError, TypeError, KeyError) as ex:
            raise ex
        unprocessed_items = response.get("UnprocessedItems", {})
        return unprocessed_items
