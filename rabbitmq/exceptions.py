class RabbitmqException(Exception):

    def __init__(self, message):
        self.message = message
        super().__init__(self.message)

    def __str__(self):
        return f'{self.__class__.__name__}({self.message})'


class RabbitmqKnownException(RabbitmqException):
    pass


class Reject(RabbitmqKnownException):
    """ Rejects to dead letter queue. calls rabbitmq.consumer.Consumer.reject()"""

    def __init__(self, message=''):
        self.message = f'Rejecting {message}'
        super().__init__(self.message)


class Debounce(RabbitmqKnownException):
    """ Debounces to delay queue if any. Use this short circuit to debounce flow"""

    def __init__(self, message=''):
        self.message = f'Debounced {message}'
        super().__init__(self.message)
