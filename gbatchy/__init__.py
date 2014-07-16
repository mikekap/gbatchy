from ._version import get_versions
__version__ = get_versions()['version']
del get_versions

from .context import batch_context, BatchGreenlet, spawn
from .batch import batched, class_batched
from .utils import pmap, pmap_unordered, pfilter, pfilter_unordered
