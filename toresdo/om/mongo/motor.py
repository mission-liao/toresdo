'''
Created on Aug 15, 2013

@author: Mission Liao
'''

from __future__ import absolute_import
from toresdo.om import ModelBase
from toresdo.om import field 
from toresdo.om import Cond
from bson import ObjectId


class Model(ModelBase):
    """
    Model for Motor, a tornado mongodb driver,
    based on pymongo
    """

    def _prepare_obj(self):        
        self._local_model = self.__class__._model.copy()

    @classmethod 
    def _prepare_cls(klass, fields):
        klass._model = {}
        for k, v in fields.items():
            klass._model.update({k: v._default})

        # reference to collection 
        if hasattr(klass, "__toresdo_db_conn__") and hasattr(klass, "__toresdo_db_name__"):
            klass._db_coll = getattr(klass.__toresdo_db_conn__[klass.__toresdo_db_name__], klass.__name__)
        else:
            raise Exception("Motor is not initialized.")

        # '_id' is builtin primary-key in mongodb
        func = lambda x: ObjectId
        func.__name__ = "_id"
        f_id = field(pk=True)
        f_id(func)
        setattr(klass, "_id", f_id)

    @classmethod
    def _is_cls_prepared(klass):
        return hasattr(klass, "_model") and klass._model != None

    def _set_field(self, name, v):
        self._local_model[name] = v

    def _get_field(self, name):
        if name in self._local_model:
            return self._local_model[name]
        # this means this field is not set and no default value
        return None

    def save(self, callback=None):
        self.__class__._db_coll.insert(self._local_model, callback=callback)

    class _Ctx(object):
        """
        context object when parsing Cond object
        to Mongo Query-BSON
        """
        def __init__(self):
            self.q = {}
            self.buf = []
            self.op = None

    @classmethod
    def _prepare_cond_ctx(klass):
        return [Model._Ctx()]

    @classmethod
    def _enter_group(klass, depth, ctx):
        ctx.append(Model._Ctx())
        return ctx

    @classmethod
    def _leave_group(klass, depth, ctx):
        if ctx[-1].op == Cond.and__:
            if (ctx[-2].op == None or ctx[-2].op == Cond.and__):
                """
                if 'and' case, we just need to insert
                all condition to parent condition. if current condition
                is root, just insert it in root.
                """
                for v in ctx[-1].buf:
                    ctx[-2].q.update(v)
            else:
                ctx[-1].q.update({"$and": ctx[-1].buf})
                ctx[-2].buf.append(ctx[-1].q)

        else:
            ctx[-1].q.update({"$or": ctx[-1].buf})
            ctx[-2].buf.append(ctx[-1].q)

        ctx.pop()
        return ctx

    @classmethod
    def _handle_cond(klass, op, fld, v2, depth, ctx):
        rec = None
        if op == Cond.lt:
            rec = {"$lt" : v2}
        elif op == Cond.le:
            rec = {"$lte" : v2}
        elif op == Cond.eq:
            rec = v2
        elif op == Cond.ne:
            rec = {"$ne": v2}
        elif op == Cond.gt:
            rec = {"$gt": v2}
        elif op == Cond.ge:
            rec = {"$gte": v2}

        ctx[-1].buf.append({fld._name: rec})
        return ctx

    @classmethod
    def _handle_group(klass, op, depth, ctx):
        if ctx[-1].op != None and ctx[-1].op != op:
            raise RuntimeError("boolean-op changed in condition-group.")

        ctx[-1].op = op
        return ctx

    @classmethod
    def _finish_cond(klass, ctx):
        if len(ctx[-1].buf) == 1:
            return ctx[-1].buf[0]
        
        ctx[-1].q.update({"$and": ctx[-1].buf})
        return ctx[-1].q

    @classmethod
    def _pre_loop(klass, stmt):
        pass
    
    @classmethod
    def _next_elm(klass, ctx):
        pass
    
    @classmethod
    def _post_loop(klass, ctx):
        pass
