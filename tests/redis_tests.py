import time
import gevent
from unittest.case import SkipTest, TestCase

from gbatchy import batch_context, spawn
from gbatchy.clients.redis import BatchRedisClient

try:
    import redis

    redis_client = redis.StrictRedis(socket_timeout=1)
    redis_client.get('hello')
except ImportError:
    print('Please install redis to run the redis client tests.')
    redis_client = None
except Exception:
    redis_client = None

class RedisClientTests(TestCase):
    def setUp(self):
        if redis_client is None:
            raise SkipTest()

        self.client = BatchRedisClient(redis_client)

        self.key_prefix = '%s|' % (time.time(),)

    def test_simple_get(self):
        @batch_context
        def get_thing(t, v):
            a = spawn(self.client.delete, self.key_prefix + 'hi' + t)
            b = spawn(self.client.set, self.key_prefix + 'hi' + t, v)
            c = spawn(self.client.get, self.key_prefix + 'hi' + t)
            _, _, result = a.get(), b.get(), c.get()
            return int(result)

        @batch_context
        def test():
            a, b = spawn(get_thing, 'a', 1), spawn(get_thing, 'b', 2)
            return a.get() + b.get()

        self.assertEquals(3, test())

    def test_pipeline(self):
        @batch_context
        def test():
            with self.client.pipeline() as p:
                p.set(self.key_prefix + 'yes', '1')
                p.set(self.key_prefix + 'yes', '2')

                self.assertIsNone(self.client.get(self.key_prefix + 'yes'))
                p.execute()
                self.assertEquals('2', self.client.get(self.key_prefix + 'yes'))

        test()
