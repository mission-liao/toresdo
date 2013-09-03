'''
Created on Aug 5, 2013

@author: Mission Liao
'''

from __future__ import absolute_import
from collections import Iterator

class Cond(object):
    """
    represent a condition used in querying database, like where-clause
    used in SQL.
    """
    
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
            n = [o for o in self._operand if type(o) != Cond]
            if len(n) != 0:
                raise TypeError("and/or non-Cond object.")

    @staticmethod
    def group(op, *args):
        """
        group a set of conditions with
        a boolean operator, like 'and' or 'or'.
        """
        if op > Cond._bool_base_:
            return Cond(op, *args)
        else:
            return None

    class _Act(object):
        """
        enum for actions used in Cond.to_cmd
        """
        _in = 0
        _out = 1
        _cond = 3
        _bool = 4

    @staticmethod
    def to_cmd(model, cond):
        """
        convert a condition to a comand that
        can be used in querying database
        """
        stmt = None
        depth = 0
        to_handle = [(Cond._Act._cond, cond)]
        """
        traverse condition-tree, and
        call corresponding hooking functions
        provided by each Model
        """
        while len(to_handle) > 0:
            rec = to_handle.pop()

            if rec[0] == Cond._Act._in:
                stmt = model._enter_depth(depth, stmt)
                depth = depth + 1
            elif rec[0] == Cond._Act._out:
                stmt = model._leave_depth(depth, stmt)
                depth = depth - 1
            elif rec[0] == Cond._Act._cond:
                c = rec[1]
                if c._op > Cond._bool_base_:
                    to_handle.append((Cond._Act._out, ))
                    to_handle.append((Cond._Act._cond, c._operand[-1]))
                    for v in reversed(c._operand[:-1]):
                        to_handle.append((Cond._Act._bool, c._op))
                        to_handle.append((Cond._Act._cond, v))
                    to_handle.append((Cond._Act._in, ))
                else:
                    stmt = model._handle_cond(c._op, c._operand[0], c._operand[1], depth, stmt)
            elif rec[0] == Cond._Act._bool:
                stmt = model._handle_bool_op(rec[1], depth, stmt)

        model._finish_cond(stmt)
        return stmt

 
class field(object):
    """
    this class represent a database field,
    used as a decorator in python.
    """
    def __init__(self, pk=False, enable_type_check=True):
        # TODO: handle primary-key
        self._pk = pk
        # TODO: add test case for type-filter
        self._type_chk = enable_type_check

        self._valid = None

    def __call__(self, fn):
        """
        this function accepts another function(that what decorator means to be)
        and receive type/default-value from that function.
        """
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
            """
            when accessed by class but not instance,
            return this field
            """
            return self

        """
        call the hook(_get_field) provided by each model to get 
        the actual value
        """
        return obj._get_field(self._name)
    
    def _check_type(self, obj, v):
        """
        private function wrapping actions required to
        check input value from caller
        """
        if self._type_chk and type(v) != self._type:
            raise Exception("Type Error.")
        if self._valid:
            v = self._valid(obj, v)

        return v

    def __set__(self, obj, v):
        if obj:
            """
            call the hook(_set_field) provided by each model to set
            the value
            """
            obj._set_field(self._name, self._check_type(obj, v))
        else:
            # TODO: add test case
            """
            change the default value of this field
            """
            self._default = self._check_type(None, v)

    def validator(self, fn):
        """
        add a new validator for this field
        """
        # TODO: replace _valid from a pointer to a list
        self._valid = fn
        return self

    """
    comparison operators
    
    These operators would generate Cond object for later usage.
    """ 
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
