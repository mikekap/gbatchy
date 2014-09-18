gbatchy
=======

[![PyPI version](https://badge.fury.io/py/gbatchy.svg)](http://badge.fury.io/py/gbatchy)

A small library inspired by batchy, but using gevent greenlets instead of yield to transfer control.

For example:

```python
from gbatchy import spawn, pget, batched, batch_context

@batched()
def batch_fn(arg_list):
    print 'In batch function with args:'
    results = []
    for args, kwargs in arg_list:
        print '\t', args[0]
        results.append(args[0] + 1)
    print 'Batch function done'
    return results

@batch_context
def fetcher():
    results = pget(batch_fn(i, as_future=True) for i in xrange(3))
    print results

@batch_context
def test():
    pget(spawn(fetcher) for i in xrange(2))
    
test()
```
would print
```
In batch function with args:
	0
	1
	2
	0
	1
	2
Batch function done
[1, 2, 3]
[1, 2, 3]
```

Mini docs:
-----------

 - `@batch_context`: Ensures that the function is running in a batch context (i.e. all concurrent calls to `@batched` functions will be coalesced)
 - `spawn(fn, *args, **kwargs)`: start a new greenlet that will run `fn(*args, **kwargs)`. This creates a batch context or uses the current one.
 - `spawn_proxy(fn, *args, **kwargs)`: same as spawn(), but returns a proxy type instead of a greenlet. This should help get rid of .get() around a lot of your code.
 - `@batched(accepts_kwargs=True)` and `@class_batched()`: marks this function as a batch function. All batch functions take just one arg: args_list: `[(args, kwargs), ...]` (or `[args, ...]` if `accepts_kwargs=False`)
 - `pget(iterable)`: a quick way to `.get()` all the arguments passed.
 - `pmap(fn, iterable)`: same as `map(fn, iterable)`, except runs in parallel. Note: keyword arguments to pmap are passed through to fn for each element.
 - `pfilter(fn, iterable)`: same as `filter(fn, iterable)` except runs in parallel.
 - `immediate(v)`: returns an `AsyncResult`-like object that is immediately ready and `immediate(v).get() is v`.
 - `immediate_exception(exc)`: same as `immediate`, but raises `exc`.
 - `transform(pending, fn)`: a somewhat low-level, but performant way to take an `AsyncResult`-like object and run `immediate(fn(pending.get()))`. Note that fn must be pure - it cannot interact with greenlets. Any extra kwargs will be passed to `fn`.
