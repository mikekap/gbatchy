## 0.4.2
 - Small performance optimization for redis.
 - Small performance optimization for transform() (& by proxy @batched functions)

## 0.4.1
 - Fix a context propagation via add_auto_wrapper when using batch functions. Also makes batch functions slightly more efficient.

## 0.4
 - Added a may_block context manager to be able to use gevent primitives between batch greenlets. For an example, see the iwait & wait implementations.
 - Add a version of iwait & wait that work in a batch context.
 - Fix *_unordered - this was previously using gevent.iwait.
 - Add a Pool implementation that mirrors gevent.pool.Pool, but works with batch greenlets. Unfortunately it was not as simple as greenlet_class=BatchGreenlet.

## 0.3.1
 - Fix redis *scan functions.

## 0.3
Features:
 - A couple of useful utils: `immediate`, `immediate_exception`, and `transform`.
 - `pmap`/`pfilter` functions now take `**kwargs` that get passed through.
 - New function `spawn_proxy`: same as `spawn`, but returns a proxy-type that evaluates to spawn().get()
 - Add `as_future=True` to @batched/@class_batched functions. Since the @batched system
   uses AsyncResult-like objects under the hood anyway, this lets you take advantage of
   that. You can save a bit of spawn() calls.
 - Add max_size to specify the maximum number of calls to coalesce.
   TODO: Add a way to specify this as e.g. sum(map(len, args_list))

Bugfixes:
 - BatchAsyncResult.wait was broken if the value was not available at the time of the call.
 - BatchAsyncResult.get has support for the timeout arg.
 - BatchAsyncResult.get_nowait exists (alias for .get(block=False))
 - BatchAsyncResult.set_exc_info now properly sets .exception (in case .get() is called multiple times).
 - BatchAsyncResult.successful no longer raises.
 - Circular dependency between context & scheduler is now gone.

## 0.2
A lot of refactoring to performance critical code.

## 0.1
Initial release
