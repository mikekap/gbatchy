from functools import wraps, partial

from .context import get_context, batch_context

@batch_context
def _batch_wait(fn_id, fn, args, dec_kwargs, as_future=False):
    future = get_context().scheduler.run_pending_batch(fn_id, fn, args, **dec_kwargs)
    return future if as_future else future.get()


def batched(accepts_kwargs=True, **dec_kwargs):
    def wrapper(fn):
        global _batch_wait
        fn_id = id(fn)

        @wraps(fn)
        def wrap_kwargs(*args, **kwargs):
            return _batch_wait(fn_id, fn, (args, kwargs), dec_kwargs,
                               as_future=kwargs.pop('as_future', False))

        @wraps(fn)
        def wrap_no_kwargs(*args, **kwargs):
            return _batch_wait(fn_id, fn, args, dec_kwargs, **kwargs)

        return wrap_kwargs if accepts_kwargs else wrap_no_kwargs
    return wrapper

def class_batched(accepts_kwargs=True, **dec_kwargs):
    def wrapper(fn):
        global _batch_wait
        fn_id = id(fn)

        @wraps(fn)
        def wrap_kwargs(self, *args, **kwargs):
            return _batch_wait((fn_id, id(self)),
                               partial(fn, self),
                               (args, kwargs),
                               dec_kwargs,
                               as_future=kwargs.pop('as_future', False))

        @wraps(fn)
        def wrap_no_kwargs(self, *args, **kwargs):
            return _batch_wait((fn_id, id(self)),
                               partial(fn, self),
                               args,
                               dec_kwargs,
                               **kwargs)

        return wrap_kwargs if accepts_kwargs else wrap_no_kwargs
    return wrapper
