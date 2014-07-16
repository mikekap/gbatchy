from functools import partial

from ..batch import class_batched

class BatchRedisClient(object):
    def __init__(self, redis_client):
        """Create a new batchy redis client.

         - redis_client: The underlying Redis/StrictRedis object
        """
        self.redis = redis_client

    def pipeline(self):
        return self.redis.pipeline()

    def brpop(self, key, timeout=0):
        raise NotImplementedError("Blocking not implemented.")

    def blpop(self, key, timeout=0):
        raise NotImplementedError("Blocking not implemented.")

    def __getattr__(self, name):
        method = partial(self._batch_call, name)
        setattr(self, name, method)
        return method

    @class_batched()
    def _batch_call(self, args_list):
        pipeline = self.redis.pipeline()

        for args, kwargs in args_list:
            getattr(pipeline, args[0])(*args[1:], **kwargs)

        return pipeline.execute()
