from .rabbitmq import _Rabbitmq


class Rabbitmq(_Rabbitmq):
    """
        Flask inspired plugin for publishing to/consuming from rabbitmq with minimal setup
    usage:
        rmq = Rabbitmq()
        rmq.init_app(app)

        or
        rmq = Rabbitmq(app)
    """

    def __init__(self, app=None, **kwargs):
        super().__init__(**kwargs)

        self.app = app
        if app is not None:
            self.init_app(app, **kwargs)

    def init_app(self, app, **kwargs):
        self._set_conn(app.config['RMS_RABBITMQ_URL'], **kwargs)
