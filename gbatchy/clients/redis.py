from functools import partial

from ..batch import class_batched

class BatchRedisClient(object):
    __slots__ = ('redis',)

    def __init__(self, redis_client):
        """Create a new batchy redis client.

         - redis_client: The underlying Redis/StrictRedis object
        """
        self.redis = redis_client

    def pipeline(self, *args, **kwargs):
        """.pipeline() is a pass-through."""
        return BatchRedisPipeline(self, *args, **kwargs)

    def brpop(self, *args, **kwargs):
        return self.redis.brpop(*args, **kwargs)

    def blpop(self, *args, **kwargs):
        return self.redis.blpop(*args, **kwargs)

    def sscan(self, *args, **kwargs):
        return self.redis.sscan(*args, **kwargs)

    def hscan(self, *args, **kwargs):
        return self.redis.hscan(*args, **kwargs)

    def zscan(self, *args, **kwargs):
        return self.redis.zscan(*args, **kwargs)

    def scan(self, *args, **kwargs):
        return self.redis.scan(*args, **kwargs)

    def flush(self):
        return self.redis.flush()

    def flushdb(self):
        return self.redis.flushdb()

    @class_batched()
    def _batch_call(self, args_list):
        with self.redis.pipeline() as pipeline:
            for args, kwargs in args_list:
                getattr(pipeline, args[0])(*args[1:], **kwargs)

            return pipeline.execute()

    def __getattr__(self, name):
        return partial(self._batch_call, name)

    @class_batched(accepts_kwargs=False)
    def _pipeline_call(self, args_list):
        return self._pipeline_call_impl(args_list)

    @class_batched(accepts_kwargs=False)
    def _silent_pipeline_call(self, args_list):
        return self._pipeline_call_impl(args_list, silent_failure=True)

    def _pipeline_call_impl(self, args_list, **kwargs):
        with self.redis.pipeline(**kwargs) as pipeline:
            slices = []
            start = 0
            for calls, in args_list:
                for fn, args, kwargs in calls:
                    getattr(pipeline, fn)(*args, **kwargs)

                end = start + len(calls)
                slices.append(slice(start, end))
                start = end

            rv = pipeline.execute()
            return map(rv.__getitem__, slices)


class BatchRedisPipeline(object):
    __slots__ = ('redis', '_batch_calls', '_silent_failure')

    def __init__(self, redis, silent_failure=False):
        self.redis = redis
        self._batch_calls = []
        self._silent_failure = silent_failure

    def _add_batch_call(self, name, *args, **kwargs):
        self._batch_calls.append((name, args, kwargs))
        return self

    def __getattr__(self, name):
        return partial(self._add_batch_call, name)

    def __enter__(self):
        return self

    def __exit__(self, typ, value, traceback):
        self._batch_calls = []

    def execute(self, **kwargs):
        try:
            if self._silent_failure:
                return self.redis._silent_pipeline_call(self._batch_calls, **kwargs)
            else:
                return self.redis._pipeline_call(self._batch_calls, **kwargs)
        finally:
            self._batch_calls = []
