from gevent import iwait

from .context import batch_context, spawn

@batch_context
def pmap(fn, items):
    return [r.get() for r in [spawn(fn, i) for i in items]]

@batch_context
def pmap_unordered(fn, items):
    """Same as the above, but returns an unordered generator that returns items as they finish."""
    return (r.get() for r in iwait([spawn(fn, i) for i in items]))

@batch_context
def pfilter(fn, items):
    return [i for r, i in [(spawn(fn, i), i) for i in items] if r.get()]

@batch_context
def pfilter_unordered(fn, items):
    """Same as the above, but returns an unordered generator that returns items as they finish."""
    return (r.get()[1] for r in iwait([spawn(lambda i: (fn(i), i), i) for i in items]) if r.get()[0])

@batch_context
def pget(lst):
    """Given a list of pending things, get()s all of them"""
    return [x.get() for x in list(lst)]
