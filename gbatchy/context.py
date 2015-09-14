from collections import OrderedDict
from contextlib import contextmanager
from functools import wraps
from gevent import Greenlet as _GeventGreenlet, getcurrent, Timeout, get_hub, iwait as _gevent_iwait
import logging
import sys

logger = logging.getLogger(__name__)

DEFAULT_SCHEDULER = None
def set_default_scheduler(cls):
    global DEFAULT_SCHEDULER
    DEFAULT_SCHEDULER = cls

class _DebugContext(object):
    """A version of the context that keeps the actual greenlets around instead
    of just counting how many are runnable."""
    def __init__(self, scheduler_class=None):
        self.hub = get_hub()
        self.greenlets = set()
        self.blocked_greenlets = set()
        self.scheduler = (scheduler_class or DEFAULT_SCHEDULER)()

        self._scheduled_callback = None

    def greenlet_created(self, g):
        assert g not in self.greenlets
        self.greenlets.add(g)
        self.blocked_greenlets.add(g)

    def greenlet_blocked(self, g):
        assert g in self.greenlets
        assert g not in self.blocked_greenlets
        self.blocked_greenlets.add(g)
        self.schedule_to_run()

    def greenlet_unblocked(self, g):
        assert g in self.blocked_greenlets
        self.blocked_greenlets.remove(g)

    def greenlet_finished(self, g):
        assert g in self.greenlets
        assert g not in self.blocked_greenlets
        self.greenlets.remove(g)
        self.schedule_to_run()

    def _maybe_run_scheduler(self):
        self._scheduled_callback = None

        if not self.greenlets and not self.scheduler.has_work():
            # Reset to stop refcycles as well as break anyone who is doing something bad.
            self.greenlets = None
            self.blocked_greenlets = None
            self.scheduler = None
            return

        if len(self.blocked_greenlets) == len(self.greenlets) and self.scheduler.has_work():
            self.scheduler.run_next()

    def schedule_to_run(self):
        if self._scheduled_callback:
            return
        self._scheduled_callback = self.hub.loop.run_callback(self._maybe_run_scheduler)


class _Context(object):
    __slots__ = ['hub', 'num_greenlets', 'num_blocked', 'scheduler', '_scheduled_callback']

    def __init__(self, scheduler_class=None):
        self.hub = get_hub()
        self.num_greenlets = 0
        self.num_blocked = 0
        self._scheduled_callback = None

        self.scheduler = (scheduler_class or DEFAULT_SCHEDULER)()

    def greenlet_created(self, g):
        self.num_greenlets += 1
        self.num_blocked += 1

    def greenlet_blocked(self, g):
        self.num_blocked += 1

        if not self._scheduled_callback and self.num_blocked == self.num_greenlets:
            self._scheduled_callback = self.hub.loop.run_callback(self._maybe_run_scheduler)

    def greenlet_unblocked(self, g):
        self.num_blocked -= 1

    def greenlet_finished(self, g):
        self.num_greenlets -= 1

        if not self._scheduled_callback and self.num_blocked == self.num_greenlets:
            self._scheduled_callback = self.hub.loop.run_callback(self._maybe_run_scheduler)

    def _maybe_run_scheduler(self):
        self._scheduled_callback = None

        if self.num_greenlets == 0 and not self.scheduler.has_work():
            # Reset to stop refcycles as well as break anyone who is doing something bad.
            self.num_greenlets = None
            self.num_blocked = None
            self.scheduler = None
            return

        if self.num_greenlets == self.num_blocked and self.scheduler.has_work():
            self.scheduler.run_next()

    def schedule_to_run(self):
        if not self._scheduled_callback:
            self._scheduled_callback = self.hub.loop.run_callback(self._maybe_run_scheduler)


CONTEXT_FACTORY = _Context
def get_context():
    return getattr(getcurrent(), 'context', None)


AUTO_WRAPPERS = []
def add_auto_wrapper(fn):
    """Adds decorator fn that wraps every function that gets called in a BatchGreenlet.
    This might be useful to e.g. propagate context."""
    AUTO_WRAPPERS.append(fn)

# We store a list of all the greenlets/other objects that are storing exc_info so we can limit the set of
# exc_infos in memory. It's a somewhat nasty hack to get rid of ref cycles.
MAX_EXC_INFOS = 10
_EXC_INFO_LIST = OrderedDict()

def add_exc_info_container(obj):
    if len(_EXC_INFO_LIST) >= MAX_EXC_INFOS:
        obj = _EXC_INFO_LIST.popitem(last=True)[0]
        obj._exc_info = (obj._exc_info[0], obj._exc_info[1], None)
    _EXC_INFO_LIST[obj] = True

def raise_exc_info_from_container(obj):
    _EXC_INFO_LIST.pop(obj, None)
    cls, val, tb = obj._exc_info
    obj._exc_info = (cls, val, None)
    raise cls, val, tb


class BatchGreenlet(_GeventGreenlet):
    def __init__(self, *args, **kwargs):
        super(BatchGreenlet, self).__init__(*args, **kwargs)

        self._links = []  # override the greenlet-native _links to use a list, which is faster for small numbers of links.

        self.context = get_context() or CONTEXT_FACTORY()
        self.context.greenlet_created(self)
        self.rawlink(self.context.greenlet_finished)

        self.is_blocked = True
        self._exc_info = ()

        for wrapper in AUTO_WRAPPERS:
            self._run = wrapper(self._run)

    def _notify_links(self):
        links = self._links
        for link in links:
            try:
                link(self)
            except:
                self.hub.handle_error((link, self), *sys.exc_info())
        del links[:]

    def awaiting_batch(self):
        assert not self.is_blocked
        self.is_blocked = True
        self.context.greenlet_blocked(self)

    def switch(self, *args, **kwargs):
        if self.is_blocked:  # There are other reasons we may be switched into, e.g. gevent.sleep().
                             # We want to ignore those (which is why we use awaiting_batch instead of switch_out, unless may_block is used).
            self.is_blocked = False
            self.context.greenlet_unblocked(self)
        return super(BatchGreenlet, self).switch(*args, **kwargs)

    def _report_error(self, exc_info):
        """Overridden to add the traceback."""
        super(BatchGreenlet, self)._report_error(exc_info)
        self._exc_info = exc_info
        add_exc_info_container(self)

    def _get(self):
        if self._exc_info[0] is not None:
            raise_exc_info_from_container(self)
        else:
            return self.value

    @property
    def exception(self):
        return self._exc_info[1] if self._exc_info else None

    def get(self, block=True):
        if block:
            if not self.ready():
                switch = getcurrent().switch
                self.rawlink(switch)
                try:
                    getattr(getcurrent(), 'awaiting_batch', lambda: None)()
                    result = self.parent.switch()
                    assert result is self, 'Invalid switch into Greenlet.join(): %r' % (result, )
                except:
                    self.unlink(switch)
                    raise
            return self._get()
        elif self.ready():
            return self._get()
        else:
            raise Timeout()

    def get_nowait(self):
        return self.get(block=False)

    def join(self, timeout=None):
        """Wait until the greenlet finishes or *timeout* expires.
        Return ``None`` regardless.
        """
        if self.ready():
            return

        switch = getcurrent().switch
        self.rawlink(switch)
        try:
            t = Timeout.start_new(timeout)
            try:
                getattr(getcurrent(), 'awaiting_batch', lambda: None)()
                result = self.parent.switch()
                assert result is self, 'Invalid switch into Greenlet.join(): %r' % (result, )
            finally:
                t.cancel()
        except Timeout as ex:
            self.unlink(switch)
            if ex is not t:
                raise
        except:
            self.unlink(switch)
            raise

    wait = join  # Compat with AsyncResult.


@contextmanager
def may_block():
    """A context manager where the operation may block waiting for another batch operation.

    This usually happens when using gevent's primitives, e.g. iwait directly. You need to
    wrap these operations in a `with may_block():`. For example, to use gevent.wait:

    ```
    def my_wait(objs, count=1):
        with may_block():
            return gevent.wait(objs, count=count)
    ```

    While under this context manager, ANY context switch out of this greenlet will be treated as a wait for a batch. For example, doing `with may_block(): gevent.sleep(1)` will cause queued @batched() functions to execute.

    You should be careful while using generators & this context manager since the blocking behavior may leak out of the generator, e.g.

    ```
    def my_iwait(objs):
        it = iwait(objs)

        while True:
            with may_block():
                yield next(it)

    for v in my_iwait([g1, g2, g3]):
        gevent.sleep(0.1)
    ```

    would cause a batch flush during a gevent.sleep, which is probably not intentional. You should instead do:

    ```
    def my_iwait(objs):
        it = iwait(objs)

        while True:
            with may_block():
                v = next(it)
            yield v

    ```
    """

    current = getcurrent()
    current_awaiting_batch = getattr(current, 'awaiting_batch', None)
    if not current_awaiting_batch:
        yield
        return

    original_switch_out = getattr(current, 'switch_out', None)
    if original_switch_out is None:
        def switch_out():
            current.awaiting_batch()
    else:
        def switch_out():
            current.awaiting_batch()
            original_switch_out()

    current.switch_out = switch_out
    try:
        yield
    finally:
        current.switch_out = original_switch_out


class BatchAsyncResult(object):
    """A slight wrapper around AsyncResult that notifies the greenlet that it's waiting for a batch result."""

    __slots__ = ['_exc_info', 'value', '_links', '_notifier']

    def __init__(self):
        self._exc_info = ()
        self.value = None
        self._links = []
        self._notifier = None

    def _notify_links(self):
        links = self._links
        for link in links:
            try:
                link(self)
            except:
                get_hub().handle_error((link, self), *sys.exc_info())
        del self._links[:]

    def set(self, value):
        self.value = value
        self._exc_info = (None, None, None)

        if self._links and not self._notifier:
            self._notifier = get_hub().loop.run_callback(self._notify_links)

    def set_exc_info(self, exc_info):
        self._exc_info = exc_info
        add_exc_info_container(self)

        if self._links and not self._notifier:
            self._notifier = get_hub().loop.run_callback(self._notify_links)

    def set_exception(self, exc):
        self.set_exc_info((type(exc), exc, None))

    def successful(self):
        """Return true if and only if it is ready and holds a value"""
        return self._exc_info and self._exc_info[0] is None

    def ready(self):
        return bool(self._exc_info)

    def _get(self):
        if self._exc_info[0]:
            raise_exc_info_from_container(self)
        else:
            return self.value

    def get(self, block=True, timeout=None):
        if timeout is not None:
            self.wait(timeout=timeout)
            if not self._exc_info:
                raise Timeout()

        if block:
            if not self._exc_info:
                switch = getcurrent().switch
                self._links.append(switch)
                try:
                    getattr(getcurrent(), 'awaiting_batch', lambda: None)()
                    result = get_hub().switch()
                    assert result is self, 'Invalid switch into AsyncResult.wait(): %r' % (result, )
                except:
                    self.unlink(switch)
                    raise
                
            return self._get()
        elif self.ready():
            return self._get()
        else:
            raise Timeout()

    def get_nowait(self):
        return self.get(block=False)

    def wait(self, timeout=None):
        if self.ready():
            return self.value
        else:
            switch = getcurrent().switch
            self.rawlink(switch)
            try:
                timer = Timeout.start_new(timeout) if timeout is not None else None
                try:
                    getattr(getcurrent(), 'awaiting_batch', lambda: None)()
                    result = get_hub().switch()
                    assert result is self, 'Invalid switch into AsyncResult.wait(): %r' % (result, )
                finally:
                    if timer is not None:
                        timer.cancel()
            except Timeout as exc:
                self.unlink(switch)
                if exc is not timer:
                    raise
            except:
                self.unlink(switch)
                raise
            # not calling unlink() in non-exception case, because if switch()
            # finished normally, link was already removed in _notify_links
        return self.value

    def rawlink(self, callback):
        """Register a callback to call when a value or an exception is set.

        *callback* will be called in the :class:`Hub <gevent.hub.Hub>`, so it must not use blocking gevent API.
        *callback* will be passed one argument: this instance.
        """
        if not callable(callback):
            raise TypeError('Expected callable: %r' % (callback, ))
        self._links.append(callback)
        if self._exc_info and not self._notifier:
            self._notifier = get_hub().loop.run_callback(self._notify_links)

    def unlink(self, callback):
        """Remove the callback set by :meth:`rawlink`"""
        try:
            self._links.remove(callback)
        except ValueError:
            pass

    # link protocol
    @property
    def exception(self):
        return self._exc_info[1] if self._exc_info else None

    def __call__(self, source):
        if source.successful():
            self.set(source.value)
        else:
            self.set_exception(source.exception)

spawn = BatchGreenlet.spawn

def batch_context(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if getattr(getcurrent(), 'context', None) is None:
            return spawn(fn, *args, **kwargs).get()
        else:
            return fn(*args, **kwargs)
    return wrapper
