from functools import partial

from ..batch import class_batched

class BatchRedisClient(object):
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

    @class_batched(accepts_kwargs=False)
    def _pipeline_call(self, args_list):
        return self._pipeline_call_impl(args_list)

    @class_batched(accepts_kwargs=False)
    def _silent_pipeline_call(self, args_list):
        return self._pipeline_call_impl(args_list, silent_failure=True)

    def _pipeline_call_impl(self, args_list, **kwargs):
        with self.redis.pipeline(**kwargs) as pipeline:
            for calls, in args_list:
                for args, kwargs in calls:
                    getattr(pipeline, args[0])(*args[1:], **kwargs)

            rv = pipeline.execute()
            results = []
            start = 0
            for call_list, in args_list:
                results.append(rv[start:start+len(call_list)])
                start += len(call_list)
            return results


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
                return self.redis._silent_pipeline_call(self._batch_calls)
            else:
                return self.redis._pipeline_call(self._batch_calls)
        finally:
            self._batch_calls = []
