import os
import sys

import django

from rabbitmq.django import Rabbitmq, handle_db_conn_close
from rabbitmq.utils import maybe_jsonify

rmq = Rabbitmq()

my_exchange = rmq.E('my-exchange', 'topic')

my_queue = rmq.Q(
    queue_name='my-queue',
    routing_key='#',
    exchange=my_exchange,
)


@maybe_jsonify
@handle_db_conn_close
def process_message(body, message):
    print(body, message)


def main():
    rmq.publish(my_queue, {"hi": "there"})

    rmq.consume(
        callback=process_message,
        queue=my_queue,
        enable_retries=False
    )


if __name__ == '__main__':
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'vendorportal.config.settings')
    django.setup()
    sys.exit(main() or 0)
