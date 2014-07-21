import sys

class Scheduler(object):
    __slots__ = []

    def add_pending_batch(self, id_, function, args_tuple, aresult):
        raise NotImplementedError()

    def run_next(self):
        raise NotImplementedError()

    def has_work(self):
        raise NotImplementedError()

    def run_batch_fn(self, fn, args, aresults):
        try:
            result = fn(args)

            if result is None:
                result = [None] * len(aresults)

            if len(result) != len(aresults):
                raise ValueError('Batch function %s did not return enough results (needed %d got %d)' % (
                    fn, len(aresults), len(result)))
        except Exception:
            result = [Raise(*sys.exc_info())] * len(aresults)

        for ar, r in zip(aresults, result):
            if isinstance(r, Raise):
                if len(r.exc_info) == 3:
                    ar.set_exc_info(r.exc_info)
                else:
                    ar.set_exception(r.exc_info[0])
            else:
                ar.set(r)

class AllAtOnceScheduler(Scheduler):
    __slots__ = ['pending_batches']

    def __init__(self):
        self.pending_batches = {}  # {id: (function, [(args, kwargs), ...], [result, result, ...])}

    def add_pending_batch(self, id_, function, args_tuple, aresult):
        if id_ not in self.pending_batches:
            self.pending_batches[id_] = (function, [args_tuple], [aresult])
        else:
            _, arg_list, aresults = self.pending_batches[id_]
            arg_list.append(args_tuple)
            aresults.append(aresult)

    def has_work(self):
        return bool(self.pending_batches)

    def run_next(self):
        assert self.pending_batches

        for function, args, aresults in self.pending_batches.itervalues():
            spawn(self.run_batch_fn, function, args, aresults)
        self.pending_batches.clear()


class Raise(object):
    """You can return this as a result of a batch function to signal throwing an exception.

    This may be useful if you want to throw only for some of the batched cases."""
    def __init__(self, *exc_info):
        self.exc_info = exc_info

# TODO: Fix this.
from .context import spawn
