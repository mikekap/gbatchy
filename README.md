gbatchy
=======

A small library inspired by batchy, but using gevent greenlets instead of yield to transfer control.

For example:

```python
from gbatchy import spawn, pget, batched, batch_context

@batched()
def batch_fn(arg_list):
    print 'Calling batch function'
    results = []
    for args, kwargs in arg_list:
        print '\t', args[0]
        results.append(args[0] + 1)
    return results

@batch_context
def fetcher():
    results = pget(spawn(batch_fn, i) for i in xrange(3))
    print results

@batch_context
def test():
    pget(spawn(fetcher) for i in xrange(2))
    
test()
```
would print
```
Calling batch function
	0
	1
	2
	0
	1
	2
[1, 2, 3]
[1, 2, 3]
```

Mini docs:
-----------

 - `spawn(fn, *args, **kwargs)`: start a new greenlet that will run fn(*args, **kwargs). This creates a batch context or uses the curren tone.
 - `@batch_context`: Ensures that the function is running in a batch context (i.e. all concurrent batch calls will be coalesced at least within the function)
 - `@batched(accepts_kwargs=True)` and `@class_batched()`: marks this function as a batch function. All batch functions take just one arg: args_list: [(args, kwargs), ...]
 - `pget(iterable)`: a quick way to .get() all the arguments passed.
 - `pmap(fn, iterable)`: same as map(fn, iterable), except runs in parallel.
 - `pfilter(fn, iterable)`: same as filter(fn, iterable) except runs in parallel.
