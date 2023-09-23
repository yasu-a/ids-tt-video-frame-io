from . import converter

__all__ = 'raw', 'gzip_compress'

raw = converter.serialize_numpy
gzip_compress = converter.serialize_numpy >> converter.gzip_buffer

