from collections import deque, OrderedDict
from gevent import Timeout, iwait as _gevent_iwait, wait as _gevent_wait, pool as _gevent_pool, queue as _gevent_queue, sleep
try:
    from peak.util.proxies import LazyProxy
except ImportError:
    from objproxies import LazyProxy
import sys

from .context import batch_context, spawn, add_exc_info_container, raise_exc_info_from_container, may_block, BatchGreenlet

@batch_context
def iwait(*args, **kwargs):
    """Same as gevent.iwait, but works with BatchGreenlets."""
    waiter = _gevent_iwait(*args, **kwargs)
    while True:
        with may_block():
            v = next(waiter)
        yield v

@batch_context
def wait(*args, **kwargs):
    """Same as gevent.wait, but works with BatchGreenlets."""
    with may_block():
        return _gevent_wait(*args, **kwargs)

@batch_context
def pget(lst):
    """Given a list of pending things, get()s all of them"""
    lst = list(lst)
    for x in lst:
        x.join()
    return [x.get() for x in lst]

@batch_context
def pmap(fn, items, **kwargs):
    return pget(spawn(fn, i, **kwargs) for i in items)

@batch_context
def pmap_unordered(fn, items, **kwargs):
    """Same as the above, but returns an unordered generator that returns items as they finish."""
    return (r.get() for r in iwait([spawn(fn, i, **kwargs) for i in items]))

@batch_context
def pfilter(fn, items, **kwargs):
    items = [(spawn(fn, i, **kwargs), i) for i in items]
    for g, _ in items:
        g.join()
    return [i for r, i in items if r.get()]

@batch_context
def pfilter_unordered(fn, items, **kwargs):
    """Same as the above, but returns an unordered generator that returns items as they finish."""
    return (r.get()[1] for r in iwait([spawn(lambda i: (fn(i, **kwargs), i), i) for i in items]) if r.get()[0])

def immediate(value):
    """Returns an AsyncResult-like object that is immediately ready and returns value."""
    return _ImmediateResult(value=value)

def immediate_exception(exception, exc_info=None):
    """Returns an AsyncResult-like object that is immediately ready and raises exception."""
    return _ImmediateResult(exception=exception, exc_info=exc_info)

def transform(pending, transformer, **kwargs):
    """Returns an AsyncResult-like that contains the result of transformer(pending).

    Note that transformer will run on the hub greenlet, so it cannot .wait/.get. If
    you do want to wait for other greenlets, just use spawn() and .get(). This is
    meant to be a highly efficient wrapper, for use in, e.g. batch operations."""
    return _TransformedResult(pending, transformer, kwargs)

def chain(pending, transformer, **kwargs):
    """Returns an AsyncResult-like that contains the result of transformer(pending).get().

    Note that transformer will run on the hub greenlet, so it cannot .wait/.get. This
    is another high-performance wrapper for operations that have several steps. If
    creating a greenlet per task is too much, consider using these, like:

    initial_add = self.mc.incr(key, as_future=True)

    def step_1(v):
        if v == None:
            return self.mc.add(key, '0', as_future=True)
        else:
            return gbatchy.immediate(v)
    """
    return _ChainedResult(pending, transformer, kwargs)

def spawn_proxy(*args, **kwargs):
    """Same as spawn(), but returns a proxy type that implicitly uses the value of
    spawn(*args, **kwargs).get().

    This may be useful to get rid of .get() calls all over your code.
    """
    return LazyProxy(spawn(*args, **kwargs).get)


class _ImmediateResult(object):
    __slots__ = ('value', '_exc_info', 'exception')

    def __init__(self, value=None, exception=None, exc_info=None):
        self.value = value
        self.exception = exc_info[1] if exc_info else exception
        self._exc_info = exc_info
        if exc_info is not None:
            add_exc_info_container(self)

    def ready(self):
        return True

    def successful(self):
        return self.exception is None

    def set(self, value=None):
        raise NotImplementedError()

    def set_exception(self, exception):
        raise NotImplementedError()

    def set_exc_info(self, exc_info):
        raise NotImplementedError()

    def get(self, block=True, timeout=None):
        if self._exc_info is not None:
            raise_exc_info_from_container(self)
        elif self.exception is not None:
            raise self.exception
        return self.value

    def wait(self, timeout=None):
        return self.value

    def get_nowait(self):
        return self.get()

    def rawlink(self, callback):
        callback(self)

    def unlink(self, callback):
        pass


class _TransformedResult(object):
    __slots__ = ('pending', 'value', 'transformer', 'kwargs', '_exc_info')

    def __init__(self, pending, transformer=None, kwargs={}):
        self.pending = pending
        self.transformer = transformer
        self.kwargs = kwargs
        self.value = None
        self._exc_info = ()

        # Links are FIFO - if others call rawlink on us, we should already
        # have called _do_transform
        pending.rawlink(self._do_transform)

    def _do_transform(self, pending):
        if self.transformer is None:
            return

        try:
            self.value = self.transformer(pending, **self.kwargs)
            self._exc_info = (None, None, None)
        except Exception as ex:
            self._exc_info = sys.exc_info()
            add_exc_info_container(self)
        finally:
            self.transformer = None

    def ready(self):
        return self.pending.ready()

    def successful(self):
        return self.pending.successful() and self._exc_info[0] is None

    def set(self, value=None):
        raise NotImplementedError()

    def set_exception(self, exception):
        raise NotImplementedError()

    def set_exc_info(self, exc_info):
        raise NotImplementedError()

    def get(self, block=True, timeout=None):
        if block:
            if timeout is None:
                self.pending.get(block=True)
            else:
                self.pending.wait(timeout=timeout)

        if self.value is None and not self.pending.ready():
            raise Timeout()

        if self._exc_info[0]:
            raise_exc_info_from_container(self)

        return self.value

    def wait(self, timeout=None):
        self.pending.wait(timeout=timeout)

        return self.value

    def get_nowait(self):
        return self.get(block=False)

    def rawlink(self, callback):
        self.pending.rawlink(lambda _: callback(self))

    def unlink(self):
        raise NotImplementedError()


class _ChainedResult(object):
    __slots__ = ('current_future', 'value', 'transformer', 'kwargs', '_exc_info', '_links')

    def __init__(self, pending, transformer=None, kwargs={}):
        self.current_future = pending
        self.transformer = transformer
        self.kwargs = kwargs
        self._links = []
        self._exc_info = ()

        pending.rawlink(self._do_transform)

    @property
    def value(self):
        return self.get()

    def _do_transform(self, pending):
        if self.transformer is None:
            return

        try:
            self.current_future = self.transformer(pending, **self.kwargs)
            self._exc_info = (None, None, None)
        except Exception as ex:
            self._exc_info = sys.exc_info()
            add_exc_info_container(self)
        finally:
            self.transformer = None
            if self.current_future.ready():
                for l in self._links:
                    l()
            else:
                for l in self._links:
                    self.current_future.rawlink(l)

    def ready(self):
        return self.current_future.ready() and self.transformer is None

    def successful(self):
        return self.current_future.successful() and self._exc_info[0] is None

    def set(self, value=None):
        raise NotImplementedError()

    def set_exception(self, exception):
        raise NotImplementedError()

    def set_exc_info(self, exc_info):
        raise NotImplementedError()

    def get(self, block=True, timeout=None):
        if block:
            f = self.current_future
            if timeout is None:
                f.get(block=True)
            else:
                f.wait(timeout=timeout)

            # If the transformer has just now been run, wait again.
            if f is not self.current_future:
                if timeout is None:
                    self.current_future.get(block=True)
                else:
                    self.current_future.wait(timeout=timeout)

        if not self.current_future.ready() or self.transformer is not None:
            raise Timeout()

        if self._exc_info[0] is not None:
            raise_exc_info_from_container(self)

        return self.current_future.get()

    def wait(self, timeout=None):
        f = self.current_future
        f.wait(timeout=timeout)
        if f is not self.current_future:
            self.current_future.wait(timeout=timeout)

        return self.current_future.value

    def get_nowait(self):
        return self.get(block=False)

    def rawlink(self, callback):
        cb = lambda _: callback(self)
        if self.transformer is None:
            self.current_future.rawlink(cb)
        else:
            self._links.append(cb)

    def unlink(self):
        raise NotImplementedError()


class Queue(_gevent_queue.Queue):
    def put(self, *args, **kwargs):
        with may_block():
            return super(Queue, self).put(*args, **kwargs)

    def get(self, *args, **kwargs):
        with may_block():
            return super(Queue, self).get(*args, **kwargs)

    def peek(self, *args, **kwargs):
        with may_block():
            return super(Queue, self).peek(*args, **kwargs)


class Pool(_gevent_pool.Pool):
    greenlet_class = BatchGreenlet

    def join(self, *args, **kwargs):
        with may_block():
            return super(Pool, self).join(*args, **kwargs)

    def kill(self, *args, **kwargs):
        with may_block():
            return super(Pool, self).kill(*args, **kwargs)

    def killone(self, *args, **kwargs):
        with may_block():
            return super(Pool, self).kill(*args, **kwargs)

    def add(self, greenlet):
        with may_block():
            return super(Pool, self).add(greenlet)

    def wait_available(self):
        with may_block():
            return super(Pool, self).wait_available()

    def apply_async(self, func, args=None, kwds=None, callback=None):
        if args is None:
            args = ()
        if kwds is None:
            kwds = {}
        if self.full():
            # cannot call spawn() directly because it will block
            return self.greenlet_class.spawn(self.apply_cb, func, args, kwds, callback)
        else:
            greenlet = self.spawn(func, *args, **kwds)
            if callback is not None:
                greenlet.link(_gevent_pool.pass_value(callback))
            return greenlet

    def map_async(self, func, iterable, callback=None):
        """
        A variant of the map() method which returns a Greenlet object.

        If callback is specified then it should be a callable which accepts a
        single argument.
        """
        return self.greenlet_class.spawn(self.map_cb, func, iterable, callback)

    def imap(self, func, iterable, **kwargs):
        """An equivalent of itertools.imap()"""
        iterable = iter(iterable)
        queue = Queue(None)
        def fill_queue():
            try:
                while True:
                    try:
                        v = next(iterable)
                    except StopIteration:
                        break
                    else:
                        queue.put(self.spawn(func, v, **kwargs))
            finally:
                queue.put(None)

        filler = self.greenlet_class.spawn(fill_queue)

        while True:
            value = queue.get()
            if value is None:
                break

            yield value.get()

        filler.get()

    def imap_unordered(self, func, iterable, **kwargs):
        """The same as imap() except that the ordering of the results from the
        returned iterator should be considered in arbitrary order."""
        iterable = iter(iterable)
        results_queue = Queue()
        num_running = [0]

        def fill_queue():
            try:
                while True:
                    try:
                        v = next(iterable)
                    except StopIteration:
                        break
                    else:
                        self.spawn(func, v, **kwargs).rawlink(results_queue.put)
                        num_running[0] += 1
            finally:
                results_queue.put(None)

        filler = self.greenlet_class.spawn(fill_queue)
        num_running[0] += 1

        while num_running[0]:
            value = results_queue.get()
            num_running[0] -= 1

            if value is None:
                continue

            yield value.get()

        filler.get()

    pmap_unordered = imap_unordered

    def pmap(self, *args, **kwargs):
        return list(self.imap(*args, **kwargs))
    map = pmap

    
