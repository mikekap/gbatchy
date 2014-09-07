import sys
from .context import spawn, BatchAsyncResult
from .utils import transform

class Scheduler(object):
    __slots__ = []

    def run_pending_batch(self, id_, function, args_tuple, **kwargs):
        raise NotImplementedError()

    def run_next(self):
        raise NotImplementedError()

    def has_work(self):
        raise NotImplementedError()

    def run_batch_fn(self, fn, args, aresult):
        try:
            result = fn(args)

            if result is None:
                result = [None] * len(args)

            if len(result) != len(args):
                raise ValueError('Batch function %s did not return enough results (needed %d got %d)' % (
                    fn, len(args), len(result)))
        except Exception:
            result = [Raise(*sys.exc_info())] * len(args)

        aresult.set(result)

class AllAtOnceScheduler(Scheduler):
    __slots__ = ['pending_batches']

    def __init__(self):
        self.pending_batches = {}  # {id: (function, [(args, kwargs), ...], [result, result, ...])}

    def run_pending_batch(self, id_, function, args_tuple, max_size=sys.maxint):
        if id_ not in self.pending_batches:
            aresult = BatchAsyncResult()
            arg_list = [args_tuple]
            self.pending_batches[id_] = (function, arg_list, aresult)
        else:
            _, arg_list, aresult = self.pending_batches[id_]
            arg_list.append(args_tuple)

        index = len(arg_list) - 1

        if index >= max_size - 1:
            self.pending_batches.pop(id_)
            spawn(self.run_batch_fn, function, arg_list, aresult)

        def result_transform(result):
            r = result[index]
            if isinstance(r, Raise):
                if len(r.exc_info) == 3:
                    exc, v, tb = r.exc_info
                    raise exc, v, tb
                else:
                    raise r.exc_info[0]
            else:
                return r

        return transform(aresult, result_transform)

    def has_work(self):
        return bool(self.pending_batches)

    def run_next(self):
        assert self.pending_batches

        for function, args, aresult in self.pending_batches.itervalues():
            spawn(self.run_batch_fn, function, args, aresult)
        self.pending_batches.clear()


class Raise(object):
    """You can return this as a result of a batch function to signal throwing an exception.

    This may be useful if you want to throw only for some of the batched cases."""
    def __init__(self, *exc_info):
        self.exc_info = exc_info
