'''
Created on Aug 5, 2013

@author: Mission Liao
'''

from __future__ import absolute_import
import collections
import abc
import inspect


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

    """
    Index to context list
    """
    i_act = 0   # action item
    i_cond = 1  # condition object
    i_p_op = 2  # bool-op of parent group
    i_op = 3    # op of current group
    i_idx = 4   # index of current condition/group in parent group

    @staticmethod
    def to_cmd(model, cond):
        """
        convert a condition to a command that
        can be used in querying database
        """
        model_ctx = model._init_cond_ctx()
        to_handle = [(Cond._Act._cond, cond, None, None, 0)]
        """
        traverse condition-tree, and
        call corresponding hooking functions
        provided by each Model
        """
        while len(to_handle) > 0:
            rec = to_handle.pop()
            if rec[Cond.i_act] == Cond._Act._in:
                # enter a condition group
                model_ctx = model._enter_group(model_ctx, rec)
            elif rec[Cond.i_act] == Cond._Act._out:
                # leave a condition group
                model_ctx = model._leave_group(model_ctx, rec)
            elif rec[Cond.i_act] == Cond._Act._cond:
                # handle a Cond
                c = rec[Cond.i_cond]
                if c._op > Cond._bool_base_:
                    idx = len(c._operand) - 1
                    to_handle.append((Cond._Act._out, None, rec[Cond.i_op], c._op, rec[Cond.i_idx]))
                    for v in reversed(c._operand):
                        to_handle.append((Cond._Act._cond, v, rec[Cond.i_op], c._op, idx))
                        idx -= 1
                    to_handle.append((Cond._Act._in, None, rec[Cond.i_op], c._op, rec[Cond.i_idx]))
                else:
                    model_ctx = model._handle_cond(c._op, c._operand[0], c._operand[1], model_ctx, rec)
            else:
                raise Exception("Unknown Case.")

        model_ctx = model._finish_cond(model_ctx)
        return model_ctx

 
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

        self._valid = []

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

        # loop through all validators
        for f in self._valid:
            f(obj, v)

        return v

    def __set__(self, obj, v):
        """
        call the hook(_set_field) provided by each model to set
        the value
        """
        obj._set_field(self._name, self._check_type(obj, v))

    def validator(self, fn):
        """
        add a new validator for this field
        """
        self._valid.append(fn)
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


class ConnPool(object):
    """
    Base implementation of Connection Pool
    """

    """
    It's old-style to initialize meta-class.
    In 3.3, correct way for metaclass should be
        class A(metaclass=abc.ABCMeta):
            ...
            
    However, Eclipse would complain about this.
    """
    __metaclass__ = abc.ABCMeta
    
    # status of connection objects
    _busy = 0
    _aval = 1   # stands for 'available'
    
    _idx_conn = 0
    _idx_status = 1

    def __init__(self, klass, max_size=5):
        self._buf = []
        self._max_sz = max_size
        self._producer = klass
        self._ctx = self._producer._new_conn_pool_ctx(self._producer.__toresdo_db_conn__)
        
    def _find_one(self):
        r = [n for n in enumerate(self._buf) if n[1][ConnPool._idx_status] == ConnPool._aval]
        # we have one available connection
        if len(r):
            self._buf[r[0][0]][ConnPool._idx_status] = ConnPool._busy
            return r[0][0]

        # we didn't have a stand-by connection, allocate a new one.
        if len(self._buf) < self._max_sz:
            self._buf.append([self._producer._new_conn(self._ctx), ConnPool._busy])
            return len(self._buf) - 1

        # maximun allowed connection is reached.
        raise OverflowError("All connection is busy and max-conn is reached.")

    def _give_back(self, key):
        if self._buf[key][ConnPool._idx_status] == ConnPool._aval:
            raise RuntimeError("Give back an available connection, maybe you release it twice.")
        
        self._buf[key][ConnPool._idx_status] = ConnPool._aval

    def _close(self):
        for v in self._buf:
            # TODO: what if the connection status is busy.
            self._producer._del_conn(self._ctx, v[0])
            
        self._buf.clear()
        self._producer._del_conn_pool_ctx(self._ctx)
        self._ctx = None

    def __getitem__(self, key):
        return self._buf[key][ConnPool._idx_conn]
    
    def __setitem__(self, key):
        """
        Avoid to modify any existing slot for connection-objects
        """
        raise Exception("Never modify connection-pool.")
    
    def __delitem__(self, key):
        """
        Avoid to modify any existing slot for connection-objects
        """
        raise Exception("Never modify connection-pool.")


    """
    in a multi-threaded project, you will need to implement
    locking mechanism in the two function below. Now the basic
    implement is no concurrent access-control and should be
    ok for tornado.
    """
    @abc.abstractmethod
    def req(self):
        raise NotImplementedError()

    @abc.abstractmethod    
    def dispose(self, key):
        raise NotImplementedError()
    
    @abc.abstractmethod
    def close_all(self):
        """
        This function should be called before the whole program
        is about to close.
        """
        raise NotImplementedError()


class DefaultConnPool(ConnPool):
    """
    Default implementation of ConnPool
    """
    def req(self):
        return self._find_one()

    def dispose(self, key):
        return self._give_back(key)
    
    def close_all(self):
        self._close()


class Session(collections.Iterator):
    """
    Session
    
    used to manage database resource, like connection.
    """
    def __init__(self, klass, cond=None, cb=None):
        self._klass = klass
        self._cond = cond
        self._cb = cb

        self._ctx = None

    def __next__(self):
        if not self._ctx:
            self._ctx = self._klass._pre_loop(Cond.to_cmd(self._klass, self._cond))
            if not self._ctx:
                raise Exception("loop initialization failed.")

        try:
            res = self._klass._next_elm(self._ctx)
            if not res:
                """
                some db-driver would only return nothing but not raise
                StopIteration exception. We unify looping behavior here.
                """
                raise StopIteration

        except StopIteration as e:
            self._klass._post_loop(self._ctx)
            self._ctx = None
            raise e

        # wrapped raw data with model
        m = self._klass()
        m._attach_model(res)
        return m


class AdapterBase(object):
    """
    Base of Adaptor.

    Each model should provide these callbacks listed below:
    ========== init model base field
    - _is_cls_inited
    - _init_cls
    - _init_obj
    - _attach_model
    ========== field access
    - _set_field
    - _get_field
    ========== compose query-statement
    - _init_cond_ctx
    - _enter_group
    - _leave_group
    - _handle_cond
    - _finish_cond
    ========== loop query result
    - _pre_loop
    - _next_elm
    - _post_loop
    ========== connection pool management
    - _cmp_conn
    _ _new_conn
    - _del_conn
    _ _new_conn_pool_ctx
    - _del_conn_pool_ctx
    """
    
    """
    Optional Callbacks
    """
    def _init_obj(self): pass
    @classmethod
    def _cmp_conn(klass, conn1, conn2): pass
    @classmethod
    def _new_conn(klass, ctx): pass
    @classmethod
    def _del_conn(klass, ctx, conn): pass
    @classmethod
    def _new_conn_pool_ctx(klass, conn_config): pass
    @classmethod
    def _del_conn_pool_ctx(klass, ctx): pass
 

    """
    Required Callbacks
    """
    @classmethod
    def _is_cls_inited(klass): raise NotImplementedError()
    @classmethod
    def _init_cls(klass, fields): raise NotImplementedError()
    @classmethod
    def _uninit_cls(klass): raise NotImplementedError()
    def _attach_model(self, model): raise NotImplementedError()
    def _set_field(self, name, v): raise NotImplementedError()
    def _get_field(self, name): raise NotImplementedError()
    @classmethod
    def _init_cond_ctx(klass): raise NotImplementedError()
    @classmethod
    def _enter_group(klass, model_ctx, ctx): raise NotImplementedError()
    @classmethod
    def _leave_group(klass, model_ctx, ctx): raise NotImplementedError()
    @classmethod
    def _handle_cond(klass, op, fld, v2, model_ctx, ctx): raise NotImplementedError()
    @classmethod 
    def _finish_cond(klass, model_ctx): raise NotImplementedError()
    @classmethod
    def _pre_loop(klass, model_ctx): raise NotImplementedError()
    @classmethod
    def _next_elm(klass, loop_ctx): raise NotImplementedError()
    @classmethod
    def _post_loop(klass, loop_ctx): raise NotImplementedError() 

    """
    Class Type of connection-pool,
    override this attribute to use your own
    connection pool
    """
    __conn_pool_cls__ = DefaultConnPool

    def __init__(self, **kwargs):
        if self.__class__.__name__ == "AdapterBase":
            raise Exception("Do not initialize AdapterBase.")
        
        if not issubclass(self.__class__.__conn_pool_cls__, ConnPool):
            raise Exception("Unknown type of connection-pool class.")

        if not self.__class__._is_cls_inited():
            """
            Preparation for connection pool
            
            We will look for any existing pool through mro,
            and make sure the connection with same address
            is not initialized yet. Once not initialized yet,
            we will allocate one.
            """
            if (not hasattr(self.__class__, "__toresdo_db_conn_table__") or
                self.__class__.__toresdo_db_conn_table__ == None):
                """
                loop MRO to find the base-class that direct inheriting
                'AdapterBase'. Once found, assign a table to it, that table is
                a list of {'connection-string, connection-pool}.
                """
                is_base_found = False
                for cls in reversed(inspect.getmro(self.__class__)):
                    if cls is AdapterBase:
                        is_base_found = True
                        continue

                    if is_base_found:
                        if issubclass(cls, AdapterBase):
                            cls.__toresdo_db_conn_table__ = []
                            break
                            
                if not hasattr(self.__class__, "__toresdo_db_conn_table__"):
                    raise Exception("Unable to create the table of connection-pools for this model {0}.".format(self.__class__.__name__))

            self.__class__.__conn_pool__ = None
            # check if connection-pool with identical config is already initialized.
            for v in self.__toresdo_db_conn_table__:
                if self.__class__._cmp_conn(self.__class__.__toresdo_db_conn__, v[0]) == 0:
                    self.__class__.__conn_pool__ = v[1]
                    break
                        
            if self.__class__.__conn_pool__ == None:
                # create a new pool based on callback
                self.__class__.__conn_pool__ = self.__class__.__conn_pool_cls__(self.__class__)

                # register this new pool
                self.__toresdo_db_conn_table__.append((self.__class__.__toresdo_db_conn__, self.__class__.__conn_pool__))

                if self.__class__.__conn_pool__ == None:
                    raise Exception("Unable to create connection pool for this model {0}".format(self.__class__.__name__))

            # generate a dict of field
            fields = {}
            for k,v in self.__class__.__dict__.items():
                if issubclass(type(v), field):
                    fields.update({k: v})

            # pass field list to model-implementation
            self.__class__._init_cls(fields)

            if not self.__class__._is_cls_inited():
                # Error check, make sure model is correctly initialized
                raise Exception("Not initialized.")

        # preparation for each instance
        self._init_obj()

        # initialize field with keyword-arguments
        for k, v in kwargs.items():
            if hasattr(self.__class__, k) and type(getattr(self.__class__, k)) is field:
                setattr(self, k, v)
                

    """
    Exported Functions
    """
    @classmethod
    def find(klass, cond=None, cb=None):
        return Session(klass, cond, cb)

    @classmethod
    def find_one(klass, cond=None, cb=None):
        return next(klass.find(cond, cb))

    @classmethod
    def release(klass):
        # find all related classes
        def get_related_classes(cls):
            sub = [cls]
            to_trace = [cls]
            while len(to_trace) > 0:
                c = to_trace.pop()

                local_sub = c.__subclasses__()
                sub.extend(local_sub)
                to_trace.extend(local_sub)
                
            return list(set(sub))
        
        # call _uninit_cls on each class
        for cls in reversed(get_related_classes(klass)):
            cls._uninit_cls()
            # release connection-pool
            if hasattr(cls, "__conn_pool__") and cls.__conn_pool__ != None:
                cls.__conn_pool__.close_all()
                cls.__conn_pool__ = None

            if hasattr(cls, "__toresdo_db_conn_table__") and cls.__toresdo_db_conn_table__ != None:
                cls.__toresdo_db_conn_table__.clear()
                cls.__toresdo_db_conn_table__ = None
    