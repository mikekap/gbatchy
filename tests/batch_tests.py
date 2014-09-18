from unittest import TestCase

import gevent
from gevent.lock import BoundedSemaphore

from gbatchy.context import spawn, batch_context, BatchAsyncResult
from gbatchy.batch import batched
from gbatchy.scheduler import Raise
from gbatchy.utils import pmap, pfilter, pmap_unordered, pfilter_unordered, spawn_proxy, transform, immediate

class BatchTests(TestCase):
    def setUp(self):
        # Quiet gevent's internal exception printing.
        self.old_print_exception = gevent.get_hub().print_exception
        gevent.get_hub().print_exception = lambda context, type, value, tb: None

    def tearDown(self):
        gevent.get_hub().print_exception = self.old_print_exception

    def test_batch_works(self):
        N_CALLS = [0]
        @batched()
        def fn(arg_list):
            N_CALLS[0] += 1
            gevent.sleep(0.01)

        @batch_context
        def test():
            g1 = spawn(fn, 1)
            g2 = spawn(fn, 2)
            g1.get()
            g2.get()
            self.assertEquals(1, N_CALLS[0])

            g1, g2 = fn(1, as_future=True), fn(2, as_future=True)
            g1.wait()
            g2.wait()
            g1.get()
            g2.get()
            self.assertEquals(2, N_CALLS[0])

        test()

    def test_batch_max_works(self):
        N_CALLS = [0]
        @batched(max_size=1)
        def fn(arg_list):
            N_CALLS[0] += 1

        @batch_context
        def test():
            g1 = spawn(fn, 1)
            g2 = spawn(fn, 2)
            g1.get()
            g2.get()
            self.assertEquals(2, N_CALLS[0])

        test()

    def test_batched_error(self):
        N_CALLS = [0]
        @batched(accepts_kwargs=False)
        def fn(arg_list):
            N_CALLS[0] += 1
            raise ValueError()

        @batch_context
        def test():
            a, b = spawn(fn, 1), spawn(fn, 2)
            self.assertRaises(ValueError, b.get)
            self.assertRaises(ValueError, a.get)

            a, b = fn(1, as_future=True), fn(2, as_future=True)
            self.assertRaises(ValueError, b.get)
            self.assertRaises(ValueError, a.get)

        test()

    def test_batched_raise(self):
        N_CALLS = [0]
        @batched(accepts_kwargs=False)
        def fn(arg_list):
            N_CALLS[0] += 1
            return [i[0] if i[0] % 2 == 0 else Raise(ValueError("You're weird")) for i in arg_list]

        @batch_context
        def test():
            a, b = spawn(fn, 1), spawn(fn, 2)
            self.assertEquals(2, b.get())
            self.assertRaises(ValueError, a.get)

            a, b = fn(1, as_future=True), fn(2, as_future=True)
            self.assertEquals(2, b.get())
            self.assertRaises(ValueError, a.get)

        test()

    def test_multipath_batch_works(self):
        N_CALLS = [0]
        @batched()
        def fn(arg_list):
            N_CALLS[0] += 1

        def get_thing_1():
            fn(1)
            fn(2)

        def get_thing_1_sleep():
            fn(1)
            gevent.sleep(0)
            fn(2)

        def get_thing_2():
            fn(2)
            gevent.sleep(0.0001)  # "block" the greenlet for e.g. a mysql query.
            fn(1)

        def test(thing_1):
            N_CALLS[0] = 0
            g1 = spawn(thing_1)
            g2 = spawn(get_thing_2)
            g1.get()
            g2.get()
            return N_CALLS[0]

        self.assertEquals(2, spawn(test, get_thing_1).get())
        self.assertEquals(2, spawn(test, get_thing_1_sleep).get())

    def test_with_gevent(self):
        CALLS = []
        @batched(accepts_kwargs=False)
        def fn(arg_list):
            CALLS.append(arg_list)

        def get_thing():
            thing = spawn(fn, 1)
            gevent.spawn(fn, 2).get()
            fn(3)
            thing.get()

        def test():
            g1, g2 = spawn(get_thing), spawn(get_thing)
            g1.get()
            g2.get()

        spawn(test).get()
        # We get 3 "batches" of calls: 2 batches of [(2,)] which are the two
        # independent gevent.spawns and 1 large batch of 1s and 3s from the
        # future.
        self.assertEquals([[(2,)], [(2,)], [(1,), (1,), (3,), (3,)]],
                          CALLS)

    def test_batch_return_value(self):
        @batched(accepts_kwargs=False)
        def fn(arg_list):
            return [x[0] for x in arg_list]

        @batch_context
        def test():
            a, b = spawn(fn, 1), spawn(fn, 2)
            return a.get(), b.get()

        self.assertEquals((1, 2), test())

    def test_concurrent_batching(self):
        lock = BoundedSemaphore(1)
        lock.acquire()  # now 0

        N_CALLS = [0]

        @batched()
        def fn(arg_list):
            N_CALLS[0] += 1
            lock.acquire()

        @batched()
        def fn2(arg_list):
            N_CALLS[0] += 1
            lock.release()

        @batch_context
        def test():
            a, b = spawn(fn), spawn(fn2)
            self.assertEquals(0, N_CALLS[0])
            a.get(), b.get()

        test()  # shouldn't hang.

    def test_utils(self):
        def add_n(i, n=1):
            return i + n

        def only_even(i):
            return i % 2 == 0

        self.assertEquals([2,3,4], pmap(add_n, [1,2,3]))
        self.assertEquals([3,4,5], pmap(add_n, [1,2,3], n=2))
        self.assertEquals([2], pfilter(only_even, [1,2,3]))

        self.assertEquals([2,3,4], sorted(pmap_unordered(add_n, [1,2,3])))
        self.assertEquals([3,4,5], sorted(pmap_unordered(add_n, [1,2,3], n=2)))
        self.assertEquals([2], list(pfilter_unordered(only_even, [1,2,3])))

        self.assertEquals(2, spawn_proxy(add_n, 1))

    def _do_test_future(self, f, complete_it=None):
        if complete_it is not None:
            self.assertFalse(f.ready())
            self.assertFalse(f.successful())
            self.assertRaises(gevent.Timeout, f.get_nowait)
            self.assertIsNone(f.wait(0.00001))

            link_data = []
            f.rawlink(lambda _: link_data.append(True))
            complete_it()
            gevent.sleep(0)
            self.assertTrue(link_data)

        self.assertTrue(f.ready())
        self.assertTrue(f.successful())
        f.wait()
        f.get_nowait()
        f.get()

        link_data = []
        f.rawlink(lambda _: link_data.append(True))
        gevent.sleep(0)
        self.assertTrue(link_data)
        
    def test_transform(self):
        def multiply_by(value, m=3):
            return value * m

        future = BatchAsyncResult()
        def finish_it():
            future.set(2)

        tfuture = transform(future, multiply_by, m=4)
        self._do_test_future(tfuture, finish_it)
        self.assertEquals(8, tfuture.get())

    def test_transform_raise(self):
        future = BatchAsyncResult()

        future.set_exception(ValueError())
        tfuture = transform(future, lambda: "NOT CALLED")
        gevent.sleep(0)

        self.assertTrue(tfuture.ready())
        self.assertFalse(tfuture.successful())
        self.assertRaises(ValueError, tfuture.get)

    def test_immediate(self):
        imm = immediate([1,2,3])
        self._do_test_future(imm)
        self.assertEquals([1,2,3], imm.get())
