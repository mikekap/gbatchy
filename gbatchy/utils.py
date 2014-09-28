from gevent import iwait, Timeout
try:
    from peak.util.proxies import LazyProxy
except ImportError:
    from objproxies import LazyProxy
import sys

from .context import batch_context, spawn, add_exc_info_container, raise_exc_info_from_container

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
    """Returns an AsyncResult-like that contains the result of transformer(pending.get()).

    Note that transformer will run on the hub greenlet, so it cannot spawn/.wait/.get. If
    you do want to spawn/wait for other greenlets, just use spawn() and .get(). This is
    meant to be a highly efficient wrapper, for use in, e.g. batch operations."""
    return _TransformedResult(pending, transformer, kwargs)

def spawn_proxy(*args, **kwargs):
    """Same as spawn(), but returns a proxy type that implicitly uses the value of
    spawn(*args, **kwargs).get().

    This may be useful to get rid of .get() calls all over your code.
    """
    return LazyProxy(spawn(*args, **kwargs).get)

class _ImmediateResult(object):
    __slots__ = ('value', 'exception', '_exc_info')

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
        if self.exception is not None:
            if self._exc_info is not None:
                raise_exc_info_from_container(self)
            else:
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
    __slots__ = ('pending', 'value', 'transformer', 'kwargs', '_exception', '_exc_info')

    def __init__(self, pending, transformer=None, kwargs={}):
        self.pending = pending
        self.transformer = transformer
        self.kwargs = kwargs
        self.value = None
        self._exception = None
        self._exc_info = None

        # Links are FIFO - if others call rawlink on us, we should already
        # have called _do_transform
        pending.rawlink(self._do_transform)

    def _do_transform(self, pending):
        if self.transformer is None:
            return

        try:
            if pending.successful():
                self.value = self.transformer(pending.get(), **self.kwargs)
            else:
                assert pending.ready()
                pending.get(block=False)
        except Exception as ex:
            self._exception = ex
            self._exc_info = sys.exc_info()
            add_exc_info_container(self)
        finally:
            self.transformer = None

    def ready(self):
        return self.pending.ready()

    def successful(self):
        return self.pending.successful() and self._exception is None

    def set(self, value=None):
        raise NotImplementedError()

    def set_exception(self, exception):
        raise NotImplementedError()

    def set_exc_info(self, exc_info):
        raise NotImplementedError()

    def get(self, block=True, timeout=None):
        if block:
            self.pending.wait(timeout=timeout)

        if self.value is None and not self.pending.ready():
            raise Timeout()

        if self._exception is not None:
            if self._exc_info is not None:
                raise_exc_info_from_container(self)
            else:
                raise self._exception

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
