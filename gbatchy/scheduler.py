import sys

from .context import spawn

def switch_if_not_dead(greenlet, value):
    if not greenlet.dead:
        greenlet.switch(value)

def kill_if_not_dead(greenlet, *exc):
    if not greenlet.dead:
        greenlet.throw(*exc)

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

            for ar, r in zip(aresults, result):
                ar.set(r)
        except Exception:
            for ar in aresults:
                ar.set_exc_info(sys.exc_info())

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
