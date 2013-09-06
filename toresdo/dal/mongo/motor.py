'''
Created on Aug 15, 2013

@author: Mission Liao
'''

from __future__ import absolute_import
from toresdo.dal import AdapterBase
from toresdo.dal import field 
from toresdo.dal import Cond
from bson import ObjectId


class Model(AdapterBase):
    """
    Model for Motor, a tornado mongodb driver,
    based on pymongo
    """

    __toresdo_db_conn__ = "localhost", 27017

    def _init_obj(self):
        self._local_model = self.__class__._model.copy()

    @classmethod 
    def _init_cls(klass, fields):
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
    def _is_cls_inited(klass):
        return hasattr(klass, "_model") and klass._model != None

    def _set_field(self, name, v):
        self._local_model[name] = v

    def _get_field(self, name):
        if name in self._local_model:
            return self._local_model[name]
        # this means this field is not set and no default value
        return None

    class _Ctx(object):
        """
        context object when parsing Cond object
        to Mongo Query-BSON
        """
        def __init__(self):
            self.q = {}
            self.buf = []

    @classmethod
    def _init_cond_ctx(klass):
        return [Model._Ctx()]

    @classmethod
    def _enter_group(klass, model_ctx, ctx):
        model_ctx.append(Model._Ctx())
        return model_ctx

    @classmethod
    def _leave_group(klass, model_ctx, ctx):
        if ctx[Cond.i_op] == Cond.and__:
            model_ctx[-1].q.update({"$and": model_ctx[-1].buf})
        else:
            model_ctx[-1].q.update({"$or": model_ctx[-1].buf})
        model_ctx[-2].buf.append(model_ctx[-1].q)
        model_ctx.pop()
        return model_ctx

    @classmethod
    def _handle_cond(klass, op, fld, v2, model_ctx, ctx):
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

        model_ctx[-1].buf.append({fld._name: rec})
        return model_ctx

    @classmethod
    def _finish_cond(klass, model_ctx):
        if len(model_ctx[-1].buf) == 1:
            return model_ctx[-1].buf[0]
        
        model_ctx[-1].q.update({"$and": model_ctx[-1].buf})
        return model_ctx[-1].q

    @classmethod
    def _pre_loop(klass, stmt):
        pass
    
    @classmethod
    def _next_elm(klass, ctx):
        pass
    
    @classmethod
    def _post_loop(klass, ctx):
        pass

    """
    Exported Functions
    """
    def save(self, callback=None):
        self.__class__._db_coll.insert(self._local_model, callback=callback)

       
# Trigger connection-pool initialization
Model()
