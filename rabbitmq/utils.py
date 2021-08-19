import datetime
import json
import logging
import pprint
import reprlib
import sys
import uuid
from functools import wraps

from .exceptions import Debounce

logger = logging.getLogger(__name__)


def add_unique_id(method):
    @wraps(method)
    def _impl(self, body, message):
        unique_message_id = message.headers.get('uuid') or str(uuid.uuid4())
        message.headers['uuid'] = unique_message_id
        message.uuid = unique_message_id
        body_repr = get_limited_repr(logger, body)
        logger.info(f'Set uuid {unique_message_id} to message: {message} with body: {body_repr}')
        return method(self, body, message)

    return _impl


def add_retry(method):
    """
    :param method: decorated function
    :param max_retries:
    :param sleep: In seconds
    """

    @wraps(method)
    def _impl(self, body, message):
        try:
            message.headers['retries'] += 1
        except KeyError:
            message.headers['retries'] = 1

        return method(self, body, message)

    return _impl


def add_error_trail(method):
    @wraps(method)
    def _impl(self, message):
        add_to_error_trail(message)
        return method(self, message)

    return _impl


def add_to_error_trail(message):
    last_exc = sys.exc_info()[1]
    msg = str(last_exc)
    if 'error_trail' in message.headers:
        message.headers['error_trail'].append(msg)
    else:
        message.headers['error_trail'] = [msg]


def get_limited_repr(log, data):
    message = "<cant show repr>"
    if log.isEnabledFor(level=logging.INFO):
        message = reprlib.repr(data)
    elif log.isEnabledFor(level=logging.DEBUG):
        message = pprint.pformat(data)
    return message


def maybe_jsonify(method):
    @wraps(method)
    def _impl(body, *args, **kwargs):
        if isinstance(body, (bytes, bytearray)):
            body = body.decode(encoding='utf-8')
        if isinstance(body, str):
            body = json.loads(body)

        return method(body, *args, **kwargs)

    return _impl


def debounce(cache, cache_key, timeout=60):
    """
    :param method: Decorated function
    :param cache: instance of cache which supports get and set
    :param cache_key: key for cache. str or callable. If callable body and message will be passed to it.
    :param timeout: timeout for cache
    :return: None/ evaluation of decorated function
    """

    def decorator(method):
        @wraps(method)
        def _impl(body, message, *args, **kwargs):
            debounced_message = message.headers.get('debounced')
            if callable(cache_key):
                key = cache_key(body, message)
            else:
                key = cache_key

            key_in_cache = cache.get(key)

            if not debounced_message and key_in_cache:
                logger.info(f'Debounce Skipping {key}')
                return
            elif not debounced_message:
                cache.set(key=key, value=datetime.datetime.utcnow(), timeout=timeout)
                message.headers['debounced'] = True
                raise Debounce(message.uuid)

            return method(body, message, *args, **kwargs)

        return _impl

    return decorator
