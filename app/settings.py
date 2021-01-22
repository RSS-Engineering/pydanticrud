from typing import Dict
from pydantic import (
    BaseSettings,
    PyObject,
)

from .backends.sqlite import Backend as SQLiteBackend


class Settings(BaseSettings):
    backends: Dict[str, PyObject] = {"default": "app.backends.sqlite.Backend"}
    initialize: bool = False

    class Config:
        env_prefix = 'cascade_'


settings = Settings()
backends = {
    name: backend
    for name, backend in settings.backends.items()
}
if 'default' not in backends:
    backends['default'] = SQLiteBackend


def get_backend(cls):
    return backends.get(cls.get_table_name(), backends['default'])(cls)
