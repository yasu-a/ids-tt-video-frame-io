from . import interface


def lookup_interface(name):
    f = getattr(interface, name)
    setattr(f, 'name', name)
    return f
