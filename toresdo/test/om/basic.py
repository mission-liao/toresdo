'''
Created on Aug 8, 2013

@author: Mission Liao
'''

import unittest
from toresdo.om import ModelBase
from toresdo.om import field

class TestModel(ModelBase):
    
    @classmethod 
    def _prepare(klass, fields):
        klass._model = {}
        for k, v in fields.items():
            klass._model.update({k: v._default})
            
    @classmethod
    def _is_prepared(klass):
        return hasattr(klass, "_model") and klass._model != None

    def _set_field(self, name, v):
        if not hasattr(self, "_local_model"):
            self._local_model = self.__class__._model.copy()
        self._local_model[name] = v

    def _get_field(self, name):
        if hasattr(self, "_local_model"):
            return self._local_model[name]
        return self.__class__._model[name]
 

class Test(TestModel):
    
    def __init__(self, max_age=1000, max_name=1000):
        super(Test, self).__init__()
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
        return v

    @field()
    def age(self):
        return 10

    @age.validator
    def age(self, v):
        if v > self._max_age:
            raise ValueError("")
        return v

class TestDB(unittest.TestCase):

    def test_basic_field(self):
        """
        basic usage of fields
        """
        m1 = Test(max_age=99, max_name=20)
        m2 = Test()

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

    def test_op_save(self):
        pass
