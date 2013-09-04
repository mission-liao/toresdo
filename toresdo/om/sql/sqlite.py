'''
Created on Aug 29, 2013

@author: Mission Liao
'''


from __future__ import absolute_import

import sqlite3
from toresdo.om import ModelBase
from toresdo.om import Cond


class Model(ModelBase):
    """
    Model for Sqlite
    
    Internally, real data is stored in a list, which is useful
    when the 'WHERE' clause is used in qmark way.
       
        SELECT * FROM User WHERE a=? and b=?
        
    We need to pass values as a iterable(list, tuple), and that's
    how we store the data.
        
    This Sqlite model is just implemented for testing, that's why
    we always connect to 'test.db' and didn't provide a way to connect
    anywhere else.
 
    """
    
    __table_name__ = "test.db"

    def _prepare_obj(self):
        self._local_val = list(self.__class__._field_default)

    @classmethod
    def _prepare_cls(klass, fields):
        # init all required variables
        klass._sql_cmd = {}
        klass._field_idx = {}

        # prepare object mapping related
        field_default = []
        idx = 0
        for k,v in fields.items():
            # prepare field index
            klass._field_idx.update({k: idx})
            idx = idx + 1
            # prepare default value list
            field_default.append(v._default)

        # convert it into tuple
        klass._field_default = tuple(field_default)

        # prepare create-table command
        cmd = "CREATE TABLE IF NOT EXISTS " + klass.__name__ + " ("
        for k, v in fields.items():
            if cmd[-1] != "(":
                cmd+= ", "
            cmd += k
            if v._type is int:
                cmd += " INTEGER"
            elif v._type is str:
                cmd += " TEXT"
        cmd += ")"
        klass._sql_cmd["create_table"] = cmd
        
        # prepare insert command
        cmd = "INSERT INTO " + klass.__name__ + " VALUES ("
        cmd += "?, " * (len(fields) - 1)    # the last qmark need special handle
        cmd += "?"
        cmd += ")"
        klass._sql_cmd["insert"] = cmd

        # create table if not exist
        conn = sqlite3.connect(klass.__table_name__)
        with conn:
            conn.execute(klass._sql_cmd["create_table"])
        conn.close()

    @classmethod
    def _is_cls_prepared(klass):
        has_idx = hasattr(klass, "_field_idx")
        has_default = hasattr(klass, "_field_default")
        if has_idx and has_default:
            return True
        elif has_idx or has_default:
            raise Exception("Failed to init SQLite Mapping")
        return False
    
    def _attach_model(self, model):
        self._local_val = model

    def _set_field(self, name, v):
        self._local_val[self.__class__._field_idx[name]] = v

    def _get_field(self, name):
        return self._local_val[self.__class__._field_idx[name]]

    def save(self, callback=None):
        conn = sqlite3.connect(self.__class__.__table_name__)
        with conn:
            conn.execute(self.__class__._sql_cmd["insert"], self._local_val)
        conn.close()
        
    @classmethod
    def _enter_depth(klass, depth, stmt):
        if stmt:
            return [stmt[0] + "(", stmt[1]]
        else:
            return ["(", []]
    
    @classmethod
    def _leave_depth(klass, depth, stmt):
        return [stmt[0] + ")", stmt[1]]
    
    @classmethod
    def _handle_bool_op(klass, op, depth, stmt):
        if op == Cond.and__:
            return [stmt[0] + " AND ", stmt[1]]
        elif op == Cond.or__:
            return [stmt[0] + " OR ", stmt[1]]
        else:
            raise Exception("Unknown boolean operator {0}".format(op))
        
    @classmethod
    def _handle_cond(klass, op, fld, v2, depth, stmt):
        if not stmt:
            stmt = ["", []]

        stmt[0] += fld._name

        if op == Cond.lt:
            stmt[0] += "<"
        elif op == Cond.le:
            stmt[0] += "<="
        elif op == Cond.eq:
            stmt[0] += "="
        elif op == Cond.ne:
            stmt[0] += "<>"
        elif op == Cond.gt:
            stmt[0] += ">"
        elif op == Cond.ge:
            stmt[0] += ">="

        stmt[0] += "?"
        stmt[1].append(v2)

        return stmt

    @classmethod 
    def _finish_cond(klass, stmt):
        stmt[0] = "SELECT * FROM " + klass.__name__ + " WHERE " + stmt[0]
        return stmt
    
    @classmethod
    def _pre_loop(klass, stmt):
        conn = sqlite3.connect(klass.__table_name__)
        curs = conn.cursor()
        curs.execute(stmt[0], stmt[1])
        
        return [conn, curs]
    
    @classmethod
    def _next_elm(klass, ctx):
        return ctx[1].fetchone()
    
    @classmethod
    def _post_loop(klass, ctx):
        ctx[0].close()
