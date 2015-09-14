from collections import defaultdict
from itertools import chain

from ..batch import class_batched
from ..utils import transform

def _get_first_future_value(d):
    return next(d.get().itervalues(), None)

class BatchMemcachedClient(object):
    __slots__ = ('client',)

    def __init__(self, client):
        self.client = client

    def get(self, key, as_future=False):
        if as_future:
            return transform(self.get_multi([key], as_future=True),
                             _get_first_future_value)
        else:
            return next(self.get_multi([key]).itervalues(), None)

    @class_batched(accepts_kwargs=False)
    def get_multi(self, keys_lists):
        """get_multi(iterable_of_keys, key_prefix=b'')"""
        saved_key_lists = []
        for args in keys_lists:
            assert len(args) == 1, 'get_multi only accepts a single argument: ' + args

            saved_key_lists.append(args[0])

        results = self.client.get_multi(frozenset(chain.from_iterable(saved_key_lists)))
        return [{k: results[k] for k in lst if k in results}
                for lst in saved_key_lists]

    def set(self, key, value, time=0, as_future=False):
        if as_future:
            def cb(v):
                return key not in v.get()
            return transform(self.set_multi({key: value}, time=time, as_future=True), cb)
        else:
            return key not in self.set_multi({key: value}, time=time)

    @class_batched()
    def set_multi(self, args_list):
        return self._do_set_command(self.client.set_multi, args_list)

    def add(self, key, value, time=0, as_future=False):
        if as_future:
            def cb(v):
                return key not in v.get()

            return transform(self.add_multi({key: value}, time=time, as_future=True), cb)
        else:
            return key not in self.add_multi({key: value}, time=time)

    @class_batched()
    def add_multi(self, args_list):
        return self._do_set_command(self.client.add_multi, args_list)

    def _do_set_command(self, fn, args):
        """add & set implementation."""
        by_time = defaultdict(dict)
        def fill_by_time(d, time=0):
            by_time[time].update(d)

        for ar, kw in args:
            fill_by_time(*ar, **kw)

        results_iter = ((fn(d, time=time), d.keys()) for time, d in by_time.iteritems())
        results_iter = (x[0] if x[0] is not None else x[1] for x in results_iter)
        failed_keys = frozenset(chain.from_iterable(results_iter))

        return [list(failed_keys & frozenset(ar[0].keys())) for ar, _ in args]

    def incr(self, key, increment=1):
        return self.client.incr(key, increment)

    def incr_multi(self, *args, **kwargs):
        return self.client.incr_multi(*args, **kwargs)

    def decr(self, key, decrement=1):
        return self.client.decr(key, decrement)

    def append(self, *args, **kwargs):
        return self.client.append(*args, **kwargs)

    def prepend(self, *args, **kwargs):
        return self.client.prepend(*args, **kwargs)

    def delete(self, key):
        self.delete_multi([key])

    @class_batched()
    def delete_multi(self, args_list):
        by_time = defaultdict(set)
        def fill_by_time(it, time=None):
            by_time[time].update(k for k in it)

        for ar, kw in args_list:
            fill_by_time(*ar, **kw)

        for time, d in by_time.iteritems():
            self.client.delete_multi(d, **({'time': time} if time is not None else {}))

    def flush_all(self):
        self.client.flush_all()
