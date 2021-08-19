from functools import wraps

from django import db
from django.conf import settings

from .rabbitmq import _Rabbitmq


class Rabbitmq(_Rabbitmq):

    def __init__(self, **kwargs):
        if not 'connection' in kwargs:
            connection = getattr(settings, 'RABBITMQ_URL', None)
            if not connection:
                raise ValueError("RABBITMQ_URL must be set with connection url in settings")
            kwargs['connection'] = connection
        super().__init__(**kwargs)


def handle_db_conn_close(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            r_val = func(*args, **kwargs)
        finally:
            db.connection.close()

        return r_val

    return wrapper
