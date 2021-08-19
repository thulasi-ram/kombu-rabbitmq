import os
import sys


from rabbitmq.flask import Rabbitmq
from rabbitmq.utils import maybe_jsonify

rmq = Rabbitmq()

my_exchange = rmq.E('my-exchange', 'topic')

my_queue = rmq.Q(
    queue_name='my-queue',
    routing_key='#',
    exchange=my_exchange,
)


@maybe_jsonify
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
    from flask import current_app
    rmq.init_app(current_app)
    sys.exit(main() or 0)
