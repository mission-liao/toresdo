'''
Created on Aug 8, 2013

@author: Mission Liao
'''

import unittest
from toresdo.dal import AdapterBase
from toresdo.dal import field
from toresdo.dal import ConnPool


class Model(AdapterBase):
    
    __toresdo_db_conn__ = "local"
    
    @classmethod 
    def _init_cls(klass, fields):
        klass._model = {}
        for k, v in fields.items():
            klass._model.update({k: v._default})
            
    @classmethod
    def _uninit_cls(klass):
        klass._model = None

    @classmethod
    def _is_cls_inited(klass):
        return "_model" in klass.__dict__ and klass._model != None

    def _set_field(self, name, v):
        if not hasattr(self, "_local_model"):
            self._local_model = self.__class__._model.copy()
        self._local_model[name] = v

    def _get_field(self, name):
        if hasattr(self, "_local_model"):
            return self._local_model[name]
        return self.__class__._model[name]

    @classmethod
    def _cmp_conn(klass, conn1, conn2):
        if conn1 == conn2:
            return 0
        return 1

    @classmethod 
    def _new_conn_pool_ctx(klass, conn_config):
        return None

    @classmethod
    def _del_conn_pool_ctx(klass, ctx):
        return None


class Test1(Model):
    
    def __init__(self, max_age=1000, max_name=1000):
        super(Test1, self).__init__()
        self._max_age = max_age
        self._max_name = max_name

    @field(pk=True)
    def name(self):
        return ""

    @name.validator
    def name(self, v):
        if not type(v) is str:
            raise ValueError("")
        if len(v) > self._max_name:
            # meaningless error, just for test
            raise RuntimeError("")

    @field()
    def age(self):
        return 10

    @age.validator
    def age(self, v):
        if v > self._max_age:
            raise ValueError("")

class TestDB(unittest.TestCase):
    
    def tearDown(self):
        # un-init everything related Model
        Model.release()

    def test_init_modelbase(self):
        with self.assertRaises(Exception):
            AdapterBase()
            
    def test_unknown_conn_pool_cls(self):
        class UnknownPool(Model):
            __conn_pool_cls__ = str
            
        with self.assertRaises(Exception):
            UnknownPool()

    def test_field_basic(self):
        """
        basic usage of fields
        """
        m1 = Test1(max_age=99, max_name=20)
        m2 = Test1()

        self.assertEqual(m1.age, 10)
        self.assertEqual(m1.name, "")
        
        m1.name = "Sarah Yeh"
        m1.age = 26
        self.assertEqual(m1.age, 26)
        self.assertEqual(m1.name, "Sarah Yeh")
        # m2 should be not touched
        self.assertEqual(m2.age, 10)
        self.assertEqual(m2.name, "")

        # make sure validator can raise exception
        with self.assertRaises(ValueError):
            m1.age = 100
        with self.assertRaises(RuntimeError):
            m1.name="ABCDEFGHIJKLMNOPQRSTUVWXYZ"
            
    def test_field_multiple_validator(self):
        """
        make sure multiple validator works
        """
        class User(Model):
            @field()
            def name(self):
                return ""
            
            @name.validator
            def name(self, v):
                if len(v) < 5:
                    raise ValueError()
                
                return v
            @name.validator
            def name(self, v):
                if len(v) > 10:
                    raise ValueError()
                
        with self.assertRaises(ValueError):
            User().name = "Tom"
            
        with self.assertRaises(ValueError):
            User().name = "TomTomTomTom"
            
    def test_multiple_conn_pool(self):
        """
        trigger to create multiple connection-pools,
        and make sure they are placed in right place.
        """
        class Conn1(Model):
            __toresdo_db_conn__ = "remote1"

        class Conn2(Model):
            __toresdo_db_conn__ = "remote2"
            
        class Conn3(Conn1):
            pass
        
        class Conn4(Conn3):
            __toresdo_db_conn__ = "remote4"
            
        class Conn5(Model):
            __toresdo_db_conn__ = "remote2"
            
        # Initialize one model to trigger _init_cls
        Conn4()
        self.assertEqual(len(Model.__toresdo_db_conn_table__), 1)
        self.assertEqual(Model.__toresdo_db_conn_table__[-1][0], "remote4")

        # Conn5 and Conn2 share the same connection-pool
        Conn5()
        self.assertEqual(len(Model.__toresdo_db_conn_table__), 2)
        Conn2()
        self.assertEqual(len(Model.__toresdo_db_conn_table__), 2)
        self.assertEqual(Model.__toresdo_db_conn_table__[-1][0], "remote2")

        # Conn3 should be able to borrow __toresdo_db_conn__ from Conn1
        Conn3()
        self.assertEqual(len(Model.__toresdo_db_conn_table__), 3)
        self.assertEqual(Model.__toresdo_db_conn_table__[-1][0], "remote1")
        
        # connection-pool should be init by Conn3, nothing changed.
        Conn1()
        self.assertEqual(len(Model.__toresdo_db_conn_table__), 3)

        # init the ultimate base class: Model
        Model()
        self.assertEqual(len(Model.__toresdo_db_conn_table__), 4)
        self.assertEqual(Model.__toresdo_db_conn_table__[-1][0], "local")

    def test_release(self):
        """
        Make sure ModelBase.release works
        """
        class MyPool(ConnPool):
            closed_cls = []
    
            def req(self):
                return 0

            def dispose(self, key):
                pass
 
            def close_all(self):
                self.__class__.closed_cls.append(self._producer)


        class Conn(Model):
            __conn_pool_cls__ = MyPool
 
        class Conn1(Conn):
            __toresdo_db_conn__ = "remote1"
            closed = False
            
            @classmethod
            def _uninit_cls(klass):
                super(Conn1, klass)._uninit_cls()
                klass.closed = True

        class Conn2(Conn):
            __toresdo_db_conn__ = "remote2"
            closed = False
            
            @classmethod
            def _uninit_cls(klass):
                super(Conn2, klass)._uninit_cls()
                klass.closed = True
            
        class Conn3(Conn):
            closed = False
            
            @classmethod
            def _uninit_cls(klass):
                super(Conn3, klass)._uninit_cls()
                klass.closed = True
        
        class Conn4(Conn):
            __toresdo_db_conn__ = "remote4"
            closed = False
            
            @classmethod
            def _uninit_cls(klass):
                super(Conn4, klass)._uninit_cls()
                klass.closed = True
            
        class Conn5(Conn):
            __toresdo_db_conn__ = "remote2"
            closed = False
            
            @classmethod
            def _uninit_cls(klass):
                super(Conn5, klass)._uninit_cls()
                klass.closed = True

        MyPool.closed_cls.clear()

        Model()
        Conn1()
        Conn2()
        Conn3()
        Conn4()
        Conn5()

        # make sure we init everything
        self.assertEqual(len(Model.__toresdo_db_conn_table__), 4)
        
        Model.release()

        self.assertTrue(Conn1.closed)
        self.assertTrue(Conn2.closed)
        self.assertTrue(Conn3.closed)
        self.assertTrue(Conn4.closed)
        self.assertTrue(Conn5.closed)
        self.assertIn(Conn1, MyPool.closed_cls)
        self.assertIn(Conn2, MyPool.closed_cls)
        self.assertNotIn(Conn3, MyPool.closed_cls)
        self.assertIn(Conn4, MyPool.closed_cls)
        self.assertNotIn(Conn5, MyPool.closed_cls)
