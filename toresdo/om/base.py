'''
Created on Aug 5, 2013

@author: Mission Liao
'''

from __future__ import absolute_import
from collections import Iterator

class Cond(object):
    """
    condition operator for value
    """
    _comp_base_ = 0
    # less than
    lt = _comp_base_ + 1
    # less than or equal
    le = _comp_base_ + 2
    # equal
    eq = _comp_base_ + 3
    # not equal
    ne = _comp_base_ + 4
    # greater than
    gt = _comp_base_ + 5
    # greater than or equal
    ge = _comp_base_ + 6
    
    """
    conditional operator for pair
    """
    _bool_base_ = 100
    # and
    and__ = _bool_base_ + 1
    # or
    or__ = _bool_base_ + 2

    def __init__(self, op, *args):
        self._op = op
        self._operand = args

        if op >= Cond._bool_base_:
            for o in self._operand:
                if type(o) != Cond:
                    raise TypeError("and/or non-Cond object.")

    @staticmethod
    def group(op, *args):
        if op > Cond._bool_base_:
            return Cond(op, *args)
        else:
            return None


    class Act(object):
        _in = 0
        _out = 1
        _cond = 3
        _bool = 4

    @staticmethod
    def to_cmd(model, cond):
        stmt = None
        depth = 0
        to_handle = [(Cond.Act._cond, cond)]
        """
        traverse condition-tree, and
        call corresponding hooking functions
        provided by each Model
        """
        while len(to_handle) > 0:
            rec = to_handle.pop()

            if rec[0] == Cond.Act._in:
                stmt = model._enter_depth(depth, stmt)
                depth = depth + 1
            elif rec[0] == Cond.Act._out:
                stmt = model._leave_depth(depth, stmt)
                depth = depth - 1
            elif rec[0] == Cond.Act._cond:
                c = rec[1]
                if c._op > Cond._bool_base_:
                    to_handle.append((Cond.Act._out, ))
                    to_handle.append((Cond.Act._cond, c._operand[-1]))
                    for v in reversed(c._operand[:-1]):
                        to_handle.append((Cond.Act._bool, c._op))
                        to_handle.append((Cond.Act._cond, v))
                    to_handle.append((Cond.Act._in, ))
                else:
                    stmt = model._handle_cond(c._op, c._operand[0], c._operand[1], depth, stmt)
            elif rec[0] == Cond.Act._bool:
                stmt = model._handle_bool_op(rec[1], depth, stmt)

        model._finish_cond(stmt)
        return stmt

 
class field(object):
    def __init__(self, pk=False, enable_type_check=True):
        # TODO: handle primary-key
        self._pk = pk
        # TODO: add test case for type-filter
        self._type_chk = enable_type_check

        self._valid = None

    def __call__(self, fn):
        # get name of this field 
        self._name = fn.__name__
        # get type of this field
        ret = fn(None)
        if type(ret) is type:
            # user pass a class, means no default value
            self._type = ret
            # TODO: test case for _default
            self._default = None
        else:
            self._type = type(ret)
            self._default = ret

        return self

    def __get__(self, obj, obj_type):
        if obj == None:
            return self

        return obj._get_field(self._name)
    
    def _check_type(self, obj, v):
        if self._type_chk and type(v) != self._type:
            raise Exception("Type Error.")
        if self._valid:
            v = self._valid(obj, v)

        return v

    def __set__(self, obj, v):
        if obj:
            obj._set_field(self._name, self._check_type(obj, v))
        else:
            # TODO: add test case
            """
            change the default value of this field
            """
            self._default = self._check_type(None, v)

    def validator(self, fn):
        self._valid = fn
        return self
    
    def __lt__(self, v):
        return Cond(Cond.lt, self, self._check_type(None, v))

    def __le__(self, v):
        return Cond(Cond.le, self, self._check_type(None, v))
    
    def __eq__(self, v):
        return Cond(Cond.eq, self, self._check_type(None, v))

    def __ne__(self, v):
        return Cond(Cond.ne, self, self._check_type(None, v))
    
    def __gt__(self, v):
        return Cond(Cond.gt, self, self._check_type(None, v))
    
    def __ge__(self, v):
        return Cond(Cond.ge, self, self._check_type(None, v))


class QSet(Iterator):
    def __init__(self, klass, cond=None, cb=None):
        self._klass = klass
        self._cond = cond
        self._cb = cb

    def __next__(self):
        # TODO: not finished yet.
        pass


class ModelBase(object):
    """
    Base of Model.
    
    Each model should provide these callbacks listed below:
    - _is_prepared
    - _prepare
    - _set_field
    - _get_field
    - _enter_depth
    - _leave_depth
    - _handle_cond
    - _handle_bool_op
    - _finish_cond
    """
    def __init__(self):
        if not self.__class__._is_prepared():
            """
            generate a dict of field
            """
            fields = {}
            for k,v in self.__class__.__dict__.items():
                if issubclass(type(v), field):
                    fields.update({k: v})

            # allow to update fields
            self.__class__._prepare(fields)

    @classmethod
    def find(klass, cond=None, cb=None):
        return QSet(klass, Cond.to_cmd(klass, cond), cb)

    @classmethod
    def find_one(klass, cond=None, cb=None):
        return next(QSet(klass, Cond.to_cmd(klass, cond), cb), None)
