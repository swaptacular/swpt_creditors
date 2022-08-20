import redis


class FlaskRedis:
    def __init__(self, app=None, strict=True, config_prefix="REDIS", **kwargs):
        self._redis_client = None
        self.provider_class = redis.StrictRedis if strict else redis.Redis
        self.provider_kwargs = kwargs
        self.config_prefix = config_prefix

        if app is not None:  # pragma: no cover
            self.init_app(app)

    def init_app(self, app, **kwargs):
        redis_url = app.config.get(
            "{0}_URL".format(self.config_prefix), "redis://localhost:6379/0"
        )

        self.provider_kwargs.update(kwargs)
        self._redis_client = self.provider_class.from_url(
            redis_url, **self.provider_kwargs
        )

        if not hasattr(app, "extensions"):  # pragma: no cover
            app.extensions = {}
        app.extensions[self.config_prefix.lower()] = self

    def __getattr__(self, name):
        return getattr(self._redis_client, name)

    def __getitem__(self, name):  # pragma: no cover
        return self._redis_client[name]

    def __setitem__(self, name, value):  # pragma: no cover
        self._redis_client[name] = value

    def __delitem__(self, name):  # pragma: no cover
        del self._redis_client[name]
