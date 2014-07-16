from functools import wraps, partial
from gevent import Greenlet as _GeventGreenlet, getcurrent, Timeout, get_hub
from gevent.event import AsyncResult as _GeventAsyncResult
from weakref import WeakSet

class _Context(object):
    def __init__(self, scheduler_class=None):
        super(_Context, self).__init__()
        self.hub = get_hub()
        self.greenlets = WeakSet()
        self.blocked_greenlets = WeakSet()

        if scheduler_class is None:
            from .scheduler import AllAtOnceScheduler
            scheduler_class = AllAtOnceScheduler
        self.scheduler = scheduler_class()

        self._scheduled_callback = None

    def greenlet_created(self, g):
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
        self.schedule_to_run()

    def greenlet_finished(self, g):
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


CONTEXT_FACTORY = _Context
def get_context():
    global getcurrent
    return getattr(getcurrent(), 'context', None)

class BatchGreenlet(_GeventGreenlet):
    def __init__(self, *args, **kwargs):
        super(BatchGreenlet, self).__init__(*args, **kwargs)

        self.context = get_context() or CONTEXT_FACTORY()
        self.context.greenlet_created(self)
        self.rawlink(self.context.greenlet_finished)

        self.is_blocked = True
        self._exc_info = None

    def awaiting_batch(self):
        assert not self.is_blocked
        self.is_blocked = True
        self.context.greenlet_blocked(self)

    def switch(self, *args, **kwargs):
        if self.is_blocked:  # There are other reasons we may be switched into, e.g. gevent.sleep().
                             # We want to ignore those (which is why we use awaiting_batch instead of switch_out).
            self.is_blocked = False
            self.context.greenlet_unblocked(self)
        return super(BatchGreenlet, self).switch(*args, **kwargs)

    def _report_error(self, exc_info):
        """Overridden to add the traceback."""
        self._exc_info = exc_info
        super(BatchGreenlet, self)._report_error(exc_info)

    def _get(self):
        if self.successful():
            return self.value
        else:
            raise self._exc_info[0], self._exc_info[1], self._exc_info[2]

    def get(self, block=True, timeout=None):
        if self.ready():
            return self._get()
        elif block:
            self.join(timeout=timeout)
            return self._get()
        else:
            raise Timeout()

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


class BatchAsyncResult(_GeventAsyncResult):
    """A slight wrapper around AsyncResult that notifies the greenlet that it's waiting for a batch result."""

    def _get(self):
        if self.successful():
            return self.value
        elif self._exc_info:
            try:
                raise self._exc_info[0], self._exc_info[1], self._exc_info[2]
            finally:
                self._exc_info = None  # break ref cycle. _exception should still work, but the TB will be wrong.
        else:
            raise self._exception

    def set_exc_info(self, exc_info):
        self._exc_info = exc_info
        self.set_exception(exc_info[0])

    def get(self, block=True, timeout=None):
        if self.ready():
            return self._get()
        elif block:
            self.wait(timeout=timeout)
            return self._get()
        else:
            raise Timeout()

    def wait(self, timeout=None):
        if self.ready():
            return self.value
        else:
            switch = getcurrent().switch
            self.rawlink(switch)
            try:
                timer = Timeout.start_new(timeout)
                try:
                    getattr(getcurrent(), 'awaiting_batch', lambda: None)()
                    result = self.hub.switch()
                    assert result is self, 'Invalid switch into AsyncResult.wait(): %r' % (result, )
                finally:
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


spawn = BatchGreenlet.spawn

def batch_context(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        is_future = kwargs.pop('future', False)
        if get_context() is None:
            future = spawn(fn, *args, **kwargs)
            return future if is_future else future.get()
        elif is_future:
            return spawn(fn, *args, **kwargs)
        else:
            return fn(*args, **kwargs)
    return wrapper
