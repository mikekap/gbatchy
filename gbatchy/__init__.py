from ._version import get_versions
__version__ = get_versions()['version']
del get_versions

from .context import batch_context, BatchGreenlet, spawn, add_auto_wrapper, set_default_scheduler
from .batch import batched, class_batched
from .scheduler import Raise
from .utils import (pmap, pmap_unordered, pfilter, pfilter_unordered, pget, immediate,
                    immediate_exception, transform, spawn_proxy, iwait, wait, Pool)

# Set up the default scheduler.
from .scheduler import AllAtOnceScheduler as DefaultScheduler
set_default_scheduler(DefaultScheduler)
