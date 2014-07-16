from functools import wraps, partial

from .context import get_context, BatchAsyncResult, batch_context


def _batch_wait(fn_id, fn, args):
    global get_context, BatchAsyncResult
    r = BatchAsyncResult()
    get_context().scheduler.add_pending_batch(fn_id, fn, args, r)
    return r.get()


def batched(accepts_kwargs=True):
    def wrapper(fn):
        global _batch_wait
        fn_id = id(fn)

        @batch_context
        @wraps(fn)
        def wrap_kwargs(*args, **kwargs):
            return _batch_wait(fn_id, fn, (args, kwargs))

        @batch_context
        @wraps(fn)
        def wrap_no_kwargs(*args):
            return _batch_wait(fn_id, fn, args)

        return wrap_kwargs if accepts_kwargs else wrap_no_kwargs
    return wrapper

def class_batched(accepts_kwargs=True):
    def wrapper(fn):
        global _batch_wait
        fn_id = id(fn)

        @batch_context
        @wraps(fn)
        def wrap_kwargs(self, *args, **kwargs):
            return _batch_wait((fn_id, id(self)),
                               partial(fn, self),
                               (args, kwargs))

        @batch_context
        @wraps(fn)
        def wrap_no_kwargs(self, *args):
            return _batch_wait((fn_id, id(self)),
                               partial(fn, self),
                               args)

        return wrap_kwargs if accepts_kwargs else wrap_no_kwargs
    return wrapper