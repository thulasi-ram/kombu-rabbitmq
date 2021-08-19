import logging
import uuid

from kombu import Connection, connections, producers
from kombu.entity import PERSISTENT_DELIVERY_MODE, Queue, Exchange

from .utils import get_limited_repr

logger = logging.getLogger(__name__)


class _Rabbitmq:
    """
        Defining a queue:
            test_queue = rmq.queue_builder(
                queue_name='test',
                routing_key='#',
                exchange='test'
            )
            or
            test_queue = rmq.queue_builder(test)

        Publishing:
            rmq.publish(test_queue, data={'test': 'hi'})
            rmq.publish(test_queue, data='hi')

        Consuming:
            def callback():
                # process message

            rmq.consume(callback=callback, test_queue)
    """
    DEFAULT_RETRY_POLICY = {
        "interval_start": 0,
        "interval_step": 5,
        "interval_max": 30,
        "max_retries": 5,
    }

    DEFAULT_PRIORITY = 0

    DEFAULT_CONNECTION_OPTIONS = {
        'transport_options': {'confirm_publish': True}
    }

    from .consumer import Consumer
    DEFAULT_CONSUMER_CLASS = Consumer

    def __init__(self, **kwargs):
        self.connection = None
        conn = kwargs.pop('connection', None)

        if conn:
            self._set_conn(conn, **kwargs)

        self.kwargs = kwargs

    def _set_conn(self, conn, **kwargs):
        connection_options = kwargs.get('connection_options', self.DEFAULT_CONNECTION_OPTIONS)
        if isinstance(conn, str):
            self.connection = Connection(conn, **connection_options)
        elif isinstance(conn, Connection):
            self.connection = conn
        else:
            raise RuntimeError('Invalid connection parameter: {c}'.format(c=conn))
        with connections[self.connection].acquire(block=True, timeout=30) as conn:
            try:
                conn.connect()
            except OSError as e:
                raise RuntimeError(f"Unable to connect to {conn}") from e

    def publish(self, exchange_or_queue, data, headers=None, routing_key=None, **kwargs):
        """
        For more info on **kwargs refer to kombu.Producer.publish
        """

        if isinstance(exchange_or_queue, Queue):
            exchange = exchange_or_queue.exchange
            routing_key = routing_key or exchange_or_queue.routing_key
        elif isinstance(exchange_or_queue, Exchange):
            exchange = exchange_or_queue
            if routing_key is None:
                raise ValueError('routing_key is mandatory if publishing to exchange')
        else:
            raise TypeError('Invalid type for exchange_or_queue')

        priority = kwargs.pop('priority', self.DEFAULT_PRIORITY)
        delivery_mode = kwargs.pop('delivery_mode', PERSISTENT_DELIVERY_MODE)
        retry_enabled = kwargs.pop('retry_enabled', True)
        retry_policy = kwargs.pop('retry_policy', self.DEFAULT_RETRY_POLICY)
        declare_entities = kwargs.pop('declare_entities', True)
        _data = self.__json__(data) if not isinstance(data, (bytes, bytearray, str)) else data

        default_headers = {
            'uuid': str(uuid.uuid4())
        }

        headers = headers or {}
        headers = {**default_headers, **headers}

        payload = {
            'exchange': exchange,
            'routing_key': routing_key,
            'body': _data,
            'headers': headers,
            'priority': priority,
            'delivery_mode': delivery_mode,
            'retry': retry_enabled,
            'retry_policy': retry_policy,
        }

        payload.update(kwargs)

        if declare_entities:
            payload['declare'] = [exchange_or_queue]

        message = get_limited_repr(logger, payload)
        logger.info('Publishing message: %s,', message)
        with connections[self.connection].acquire(block=True, timeout=30) as conn:
            with producers[conn].acquire(block=True, timeout=30) as producer:
                producer.publish(**payload)

    def consume(self, callback, queue, **kwargs):
        """
        For more info on **kwargs refer to rabbitmq.consumer.Consumer
        """
        consumer_class = kwargs.get('consumer_class') or self.DEFAULT_CONSUMER_CLASS
        with connections[self.connection].acquire(block=True, timeout=30) as conn:
            consumer = consumer_class(self, conn, callback, queue, **kwargs)
            logger.info(f"Consuming from {queue}")
            consumer.run()

    def queue_builder(self, queue_name, routing_key='#', exchange='', **kwargs):
        if isinstance(exchange, str):
            exchange_options = kwargs.get('exchange_options', {})
            exchange = self._exchange_builder(exchange=exchange, **exchange_options)

        queue_options = kwargs.get('queue_options', {})
        queue = self._queue_builder(queue_name=queue_name, routing_key=routing_key, exchange=exchange, **queue_options)
        return queue

    Q = queue_builder  # just a syntactic sugar for queue builder

    def _queue_builder(self, queue_name, routing_key='#', exchange=None, durable=True, **kwargs):
        return Queue(
            name=queue_name,
            routing_key=routing_key,
            exchange=exchange,
            channel=self.connection,
            durable=durable,
            **kwargs
        )

    def _exchange_builder(self, exchange='', type=None, durable=True, **kwargs):
        return Exchange(
            name=exchange,
            type=type,
            durable=durable,
            channel=self.connection,
            **kwargs)

    E = _exchange_builder  # just a syntactic sugar for exchange builder

    def __json__(self, data):
        import json
        json_module = self.kwargs.get('json_module', json)
        json_options = self.kwargs.get('json_options', {})
        return json_module.dumps(data, **json_options)
