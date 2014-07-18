from functools import partial

from ..batch import class_batched
from ..context import spawn
from ..utils import pget

class BatchRedisClient(object):
    def __init__(self, redis_client):
        """Create a new batchy redis client.

         - redis_client: The underlying Redis/StrictRedis object
        """
        self.redis = redis_client

    def pipeline(self, *args, **kwargs):
        """.pipeline() is a pass-through."""
        return BatchRedisPipeline(self, *args, **kwargs)

    def brpop(self, key, timeout=0):
        return self.redis.brpop(key, timeout)

    def blpop(self, key, timeout=0):
        return self.redis.blpop(key, timeout)

    def flush(self):
        return self.redis.flush()

    def flushdb(self):
        return self.redis.flushdb()

    def __getattr__(self, name):
        method = partial(self._batch_call, name)
        setattr(self, name, method)
        return method

    @class_batched()
    def _batch_call(self, args_list):
        with self.redis.pipeline() as pipeline:
            for args, kwargs in args_list:
                getattr(pipeline, args[0])(*args[1:], **kwargs)

            return pipeline.execute()

    @class_batched()
    def _silent_batch_call(self, args_list):
        with self.redis.pipeline(silent_failure=True) as pipeline:
            for args, kwargs in args_list:
                getattr(pipeline, args[0])(*args[1:], **kwargs)

            return pipeline.execute()


class BatchRedisPipeline(object):
    def __init__(self, redis, silent_failure=False):
        self.redis = redis
        self._batch_calls = []
        self._silent_failure = silent_failure

    def __getattr__(self, name):
        method = partial(self._add_batch_call, name)
        setattr(self, name, method)
        return method

    def _add_batch_call(self, *args, **kwargs):
        self._batch_calls.append((args, kwargs))
        return self

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self._batch_calls = []

    def execute(self):
        try:
            if self._silent_failure:
                return pget(spawn(self.redis._silent_batch_call, *a, **kw) for a, kw in self._batch_calls)
            else:
                return pget(spawn(self.redis._batch_call, *a, **kw) for a, kw in self._batch_calls)
        finally:
            self._batch_calls = []
