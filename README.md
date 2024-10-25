# PydantiCRUD

Supercharge your Pydantic models with CRUD methods and a pluggable backend

pydanticrud let's you add a few details to your pydantic models to get basic
CRUD methods automatically built into your models for the backend of your
choice.

## Usage

```python
from pydanticrud import BaseModel, DynamoDbBackend

class User(BaseModel):
    id: int
    name: str

    class db_config:
        table_name = 'User'
        backend = DynamoDbBackend
        hash_key = 'id'
```

First, use the `BaseModel` from `pydanticrud` instead of `pydantic`.

Next add your backend to your model's `db_config` class. PydantiCRUD is geared
toward DynamoDB but provides SQLite for lighter-weight usage. You can provide
your own if you like.

Finally, add appropriate members to the `db_config` class for the chosen backend.

## Methods

### Model Methods

`get(id)` - return and instance of the Model from the backend indexed by `id`.

`exists()` - detect if the model (table) exists in the backend

`initialize()` - setup the model (table) in the backend

`delete(id)` - delete the record from backend

`query(rule)` - return a list of records that satify the rule. Rules are
defined by [rule-engine](https://zerosteiner.github.io/rule-engine/) for
querying or filtering the backend.

NOTE: Rule complexity is limited by the querying capabilities of the backend.

### Instance Methods

`save()` - store the Model instance to the backend

`batch_save()` - store the List of Model instance to the backend

### DynamoDB

`get(key: Union[Dict, Any])`

- `key` can be any of 3 types:
  - in the case of a single hash_key, a value of type that matches the hash_key
  - in the case of a hash and range key, a tuple specifying the respective values
  - a dictionary of the hash and range keys with their names and values. This method can pull for alternate indexes.

`query(query_expr: Optional[Rule], filter_expr: Optional[Rule], limit: Optional[str], exclusive_start_key: Optional[tuple[Any]], order: str = 'asc'`

- Providing a `query_expr` parameter will try to apply the keys of the expression to an
  existing index.
- Providing a `filter_expr` parameter will filter the results of
  a passed `query_expr` or run a dynamodb `scan` if no `query_expr` is passed.
- An empty call to `query()` will return the scan results (and be resource
  intensive).
- Providing a `limit` parameter will limit the number of results. If more results remain, the returned dataset will have an `last_evaluated_key` property that can be passed to `exclusive_start_key` to continue with the next page.
- Providing `order='desc'` will return the result set in descending order. This is not available for query calls that "scan" dynamodb.

`count(query_expr: Optional[Rule], exclusive_start_key: Optional[tuple[Any]], order: str = 'asc'`

- Same as `query` but returns an integer count as total. (When calling `query` with a limit, the count dynamodb returns is <= the limit you provide)

## Backend Configuration Members

`hash_key` - the name of the key field for the backend table

### DynamoDB

`hash_key` - the name of the [partition key](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.CoreComponents.html#HowItWorks.CoreComponents.PrimaryKey) field for the backend table.

`range_key` - (optional) the name of the [sort key](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.CoreComponents.html#HowItWorks.CoreComponents.PrimaryKey) field for the backend table for composite key tables.

`region` - (optional) specify the region to access dynamodb in.

`endpoint` - (optional) specify an endpoint to use a local or non-AWS implementation of DynamoDB

`local_indexes` - (optional) specify a mapping of index-name to tuple(partition_key).

`global_indexes` - (optional) specify a mapping of index-name to tuple(partition_key).

`ttl` - (optional) the name of the datetime-typed field that dynamo should consider to be the TTL field. PydantiCRUD will save this field as a float type instead of an ISO datetime string. This field only works properly with UTC-zoned datetime instances.

### SQLite (Python 3.7+)

`database` - the filename of the database file for SQLite to use

## Roadmap

There is plenty of room for improvement to PydantiCRUD.

- Backend feature support not being consistent is the most egregious flaw that
can be incrementally improved.
- SQLite JSON1 extension support for more powerful queries.
- Add more backends

  - REST API
  - Redis
  - Postgres/MySQL

## Tools

### Testing

You can run unittests with `poetry run pytest`

### Formatting

You can format the code by running:

`for d in pydanticrud tests; do poetry run black $d; done`
