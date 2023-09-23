import gzip
import pickle

import numpy as np
import io
from .core import SingleArrayInterface

__all__ = 'serialize_numpy', 'gzip_buffer'


def _numpy_to_bytes(obj, **kwargs):
    assert isinstance(obj, np.ndarray)
    with io.BytesIO() as mem:
        np.save(mem, obj)
        return mem.getvalue()


def _bytes_to_numpy(obj, **kwargs):
    assert isinstance(obj, bytes)
    return pickle.loads(obj)


serialize_numpy = SingleArrayInterface(
    forward=_numpy_to_bytes,
    backward=_bytes_to_numpy
)


def _bytes_to_gzip(obj, **kwargs):
    assert isinstance(obj, bytes)
    return gzip.compress(obj)


def _gzip_to_bytes(obj, **kwargs):
    assert isinstance(obj, bytes)
    return gzip.decompress(obj)


gzip_buffer = SingleArrayInterface(
    forward=_bytes_to_gzip,
    backward=_gzip_to_bytes
)
