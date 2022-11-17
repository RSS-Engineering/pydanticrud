from copy import copy
from datetime import datetime
from typing import Optional, Dict, Any
import boto3
from opensearchpy import AWSV4SignerAuth, OpenSearch, RequestsHttpConnection, NotFoundError
from rule_engine import Rule

from pydanticrud import DoesNotExist



# exists

class Backend:
    client = None

    def __init__(self, cls, db_backend=None):
        self.cls = cls
        cfg = cls.Config
        self.cfg = cfg
        self.host = cfg.host
        self.region = getattr(cfg, "region", "us-east-2")  # e.g. us-west-1
        if db_backend:
            self.db_backend = db_backend(self.cls)

        credentials = boto3.Session().get_credentials()
        # self.auth = AWSV4SignerAuth(credentials, self.region)
        # client = OpenSearch(
        #     hosts=[{'host': host, 'port': 443}],
        #     http_auth=auth,
        #     use_ssl=True,
        #     verify_certs=True,
        #     connection_class=RequestsHttpConnection
        # )
        self.auth = ('admin', 'admin')

    def initialize(self):
        if self.db_backend:
            self.db_backend.initialize()
        client = OpenSearch(
            hosts=[{'host': self.host, 'port': 9200}],
            http_compress=True,
            http_auth=self.auth,
            use_ssl=True,
            verify_certs=False,
            ssl_assert_hostname=False,
            ssl_show_warn=False,
        )
        self.client = client
        try:
            return client.info()
        except Exception as e:
            raise ConnectionError(f'could not connect to opensearch instance: {e}')

    def query(self, key, **kwargs):
        size = kwargs.get('size', 5)
        extra_fields = kwargs.get('extra_fields', False)
        if not extra_fields:
            fields: dict = copy(vars(self.cls).get("__fields__"))
            source_includes = list(fields.keys())
            index = self.cfg.os_index
        query = {
            'size': size,
            'query': {
                'query_string': {
                    'default_field': 'body',
                    'query': key
                }
            }
        }
        try:
            res = self.client.search(
                body=query,
                index=index,
                _source_includes=source_includes
            )
        except NotFoundError:
            raise NotFoundError(f"document with id {id} not found")
        result = self.from_es(res)
        return result

    def get(self, key, **kwargs):
        res = self.query(key, **kwargs)
        if len(res) > 0:
            return res[0]
        raise DoesNotExist(f'{self.cfg.os_index} with {key} does not exist')

    def save(self, doc, index_name: Optional[str] = None, wait_for: Optional[bool] = False, **kwargs):
        if self.db_backend:
            self.db_backend.save(doc, kwargs)
        doc = self.to_es(doc)
        if not index_name:
            index_name = self.cfg.os_index
        refresh = "false"
        if wait_for:
            refresh = "wait_for"
        res = self.client.index(
            index=index_name,
            body=doc,
            id=doc.get('id'),
            refresh=refresh
        )
        return res["_id"] == doc["id"]

    def delete(self, id, index: Optional[str] = None, **kwargs):
        wait_for = kwargs.get("wait_for", False)
        if not id:
            raise ValueError("id missing from object")
        refresh = "false"
        if wait_for:
            refresh = "wait_for"
        if not index:
            index = self.cfg.os_index

        try:
            self.client.delete(index=index, id=id, refresh=refresh)
        except NotFoundError:
            raise NotFoundError(f"document with id {id} not found")

    def to_es(self, doc, **kwargs) -> Dict:
        exclude_unset = kwargs.pop(
            "exclude_unset",
            False,
        )

        exclude: set = kwargs.pop("exclude", {})

        d = doc.dict(exclude=exclude, exclude_unset=exclude_unset, **kwargs)
        for k, v in d.items():
            if isinstance(v, datetime):
                d[k] = v.isoformat()

        return d

    def from_es(self, data: Dict[str, Any]) -> list:
        if not data:
            return None

        data = data['hits']['hits']
        result = []
        for dt in data:
            source = dt.get("_source")
            source['id'] = 1
            if not source:
                raise
            model = self.cls(**source)
            result.append(model)
        return result
