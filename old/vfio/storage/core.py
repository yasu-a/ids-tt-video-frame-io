class AbstractArrayInterface:
    def forward(self, obj, **kwargs):
        raise NotImplementedError()

    def backward(self, obj, **kwargs):
        raise NotImplementedError()

    def _chain(self):
        raise NotImplementedError()

    def __rshift__(self, other: 'AbstractArrayInterface'):
        interfaces = *self._chain(), *other._chain()
        return ChainedArrayInterface(*interfaces)


class SingleArrayInterface(AbstractArrayInterface):
    def __init__(self, forward, backward):  # i2o, o2i
        self.__forward = forward
        self.__backward = backward

    def forward(self, obj, **kwargs):
        return self.__forward(obj, **kwargs)

    def backward(self, obj, **kwargs):
        return self.__backward(obj, **kwargs)

    def _chain(self):
        return [self]


class ChainedArrayInterface(AbstractArrayInterface):
    def __init__(self, *interfaces):
        self.__interfaces = interfaces

    def forward(self, obj, **kwargs):
        for m in self.__interfaces:
            obj = m.forward(obj, **kwargs)
        return obj

    def backward(self, obj, **kwargs):
        for m in reversed(self.__interfaces):
            obj = m.backward(obj, **kwargs)
        return obj

    def _chain(self):
        return self.__interfaces
