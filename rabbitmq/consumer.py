import logging
import socket

from kombu import Queue, Exchange
from kombu.mixins import ConsumerMixin

from .exceptions import Reject, Debounce, RabbitmqKnownException
from .utils import add_unique_id, add_retry, add_error_trail

logger = logging.getLogger(__name__)


class Consumer(ConsumerMixin):
    """
    Consumer wrapper class for kombu.ConsumerMixin

    Features:
        1. Supports dead letter queue by default
        2. Supports delayed retries and error trails when enabled
        3. Adds unique ids to messages
        4. Adds hostname as tag

    Usage:
        consumer = Consumer(conn, callback, queue, **kwargs)
        consumer.run()

        with delayed retries:
            consumer = Consumer(conn, callback, queue, enable_retries=True, max_retries=2, delay=60, **kwargs)
            consumer.run()

    """
    DEFAULT_ENABLE_RETRIES = False
    DEFAULT_MAX_RETRIES = 3
    DEFAULT_DELAY_ON_RETRY = 60  # in seconds
    DEFAULT_PREFETCH_COUNT = 1
    DEFAULT_TAG = socket.gethostname()

    def __init__(self, rmq, connection, callback, queue, **kwargs):

        self.rmq = rmq
        self.connection = connection
        self.callback = callback
        self.kwargs = kwargs

        self.queue = queue
        self.queue.maybe_bind(connection)  # bind to conn if queue was not made using conn
        self.queue.declare()

        queue_name = self.queue.name
        routing_key = self.queue.routing_key

        dead_exchange = Exchange(f"{queue_name}.dead")

        self.dead_letter_queue = Queue(
            name=f"{queue_name}.dead",
            routing_key=f"dead.{routing_key}",
            exchange=dead_exchange,
            channel=self.connection
        )
        self.dead_letter_queue.declare()

        self.enable_retries = self.kwargs.get('enable_retries') or self.DEFAULT_ENABLE_RETRIES
        if self.enable_retries:
            delay_exchange = Exchange(f"{queue_name}.delay", channel=self.connection)
            self.delay_queue = Queue(
                name=f"{queue_name}.delay",
                routing_key=f"delay.{routing_key}",
                exchange=dead_exchange,
                channel=self.connection,
                durable=True,
                queue_arguments={
                    'x-dead-letter-exchange': delay_exchange.name,
                    "x-dead-letter-routing-key": routing_key
                }
            )
            self.delay_queue.declare()
            delay_exchange.declare()
            self.queue.bind_to(delay_exchange, routing_key)

    def get_consumers(self, consumer, channel):
        prefetch_count = self.kwargs.get('prefetch_count', self.DEFAULT_PREFETCH_COUNT)
        return [
            consumer(
                queues=[self.queue],
                callbacks=[self._on_task],
                prefetch_count=prefetch_count,
                tag_prefix=f'({self.DEFAULT_TAG})-'
            )
        ]

    def reject(self, message):
        logger.info(f'Rejecting {message.uuid}')
        self.rmq.publish(
            exchange_or_queue=self.dead_letter_queue,
            data=message.body,
            headers=message.headers,
        )

    def requeue(self, message):
        logger.info(f'Requeuing {message.uuid}')
        expiration = self.kwargs.get('delay', self.DEFAULT_DELAY_ON_RETRY)
        self.rmq.publish(
            exchange_or_queue=self.delay_queue,
            data=message.body,
            headers=message.headers,
            expiration=expiration,
        )

    @add_error_trail
    def requeue_or_reject(self, message):
        max_retries = self.kwargs.get('max_retries', self.DEFAULT_MAX_RETRIES)
        if self.enable_retries and message.headers['retries'] < max_retries:
            return self.requeue(message)
        return self.reject(message)

    @add_unique_id
    @add_retry
    def _on_task(self, body, message):
        try:
            self.callback(body, message)
        except Reject as e:
            logger.error(f'{e}')
            self.reject(message=message)
        except Debounce as e:
            logger.error(f'{e}')
            self.requeue(message=message)
        except Exception as e:
            log_error = logger.error if isinstance(e, RabbitmqKnownException) else logger.exception
            log_error(f'Exception in {message.uuid} due to {e}')
            self.requeue_or_reject(message)

        message.ack()
        logger.info(f'Acked {message.uuid}')
