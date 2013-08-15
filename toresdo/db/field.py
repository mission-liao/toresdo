'''
Created on Aug 8, 2013

@author: Mission Liao
'''


class field(object):
    def __init__(self, pk=False):
        self._pk = pk

    def __call__(self, fn):
        # get name of this field 
        self._name = fn.__name__
        # get type of this field
        ret = fn(None)
        if type(ret) is type:
            # no default value
            self._type = ret
            self._default = None
        else:
            self._type = type(ret)
            self._default = ret

        return self


    def __get__(self, obj, obj_type):
        if obj == None:
            return self

        return obj._get(self._name)

    def __set__(self, obj, v):
        if obj:
            if self._filter:
                v = self._filter(v)
            elif type(v) != self._type:
                raise Exception("Type Error.")

            obj._set(self._name, v)

    def filter(self, fn):
        self._filter = fn
        return self
