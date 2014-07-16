import time
from unittest.case import SkipTest, TestCase

from gbatchy import spawn
from gbatchy.clients.memcached import BatchMemcachedClient

try:
    import pylibmc

    mc_client = pylibmc.Client(["127.0.0.1"], behaviors={
        'connect_timeout': 1
    })
    mc_client.get('hello')
except ImportError:
    print('Please install pylibmc to run the memcached client tests.')
    mc_client = None
except Exception:
    mc_client = None

class MemcachedClientTests(TestCase):
    def setUp(self):
        if mc_client is None:
            raise SkipTest()

        self.client = BatchMemcachedClient(mc_client)

        self.key_prefix = ('%s|' % (time.time(),)).encode('ascii')

    def test_multi_get(self):
        def set_thing(a, b):
            self.client.set(self.key_prefix + b'hi' + a, b, time=100)

        def get_thing(a):
            return self.client.get(self.key_prefix + b'hi' + a)

        def test():
            a, b = spawn(set_thing, b'a', 1), spawn(set_thing, b'b', 2)
            a.get(), b.get()
            a, b = spawn(get_thing, b'a'), spawn(get_thing, b'b')
            return a.get() + b.get()

        self.assertEquals(3, test())

    def test_multi_delete(self):
        def set_thing(a, b):
            self.client.set(self.key_prefix + b'hi' + a, b, time=100)

        def get_thing(a):
            return self.client.get(self.key_prefix + b'hi' + a)

        def delete_thing(a):
            self.client.delete(self.key_prefix + b'hi' + a)

        def test():
            a, b = spawn(set_thing, b'a', 1), spawn(set_thing, b'b', 2)
            a.get(), b.get()
            a, b = spawn(delete_thing, b'a'), spawn(delete_thing, b'b')
            a.get(), b.get()
            a, b = spawn(get_thing, b'a'), spawn(get_thing, b'b')
            return (a.get(), b.get())

        self.assertEquals((None, None), test())

    def test_other_methods(self):
        self.assertEquals(1, self.client.add(self.key_prefix + b'hello', 0))
        self.assertEquals(0, self.client.add(self.key_prefix + b'hello', 0))
        self.assertEquals([self.key_prefix + b'hello'],
                          self.client.add_multi({self.key_prefix + b'hello': 0}))
        self.assertEquals(1, self.client.incr(self.key_prefix + b'hello', 1))
        self.assertEquals(0, self.client.decr(self.key_prefix + b'hello', 1))

        self.client.incr_multi([self.key_prefix + b'hello'], delta=5)
        self.assertEquals(5, self.client.get(self.key_prefix + b'hello'))

        self.client.append(self.key_prefix + b'hello', b'0')
        self.assertEquals(50, self.client.get(self.key_prefix + b'hello'))

        self.client.prepend(self.key_prefix + b'hello', b'1')
        self.assertEquals(150, self.client.get(self.key_prefix + b'hello'))
