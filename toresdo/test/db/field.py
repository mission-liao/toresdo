'''
Created on Aug 8, 2013

@author: Mission Liao
'''


import unittest

from toresdo.db import field
from toresdo.db.base import ModelBase

class TestModel(ModelBase):
    
    @classmethod 
    def _prepare(klass, fields):
        klass._model = {}
        for k, v in fields.items():
            klass._model.update({k: v._default})

    def _set(self, name, v):
        if not hasattr(self, "_local_model"):
            self._local_model = self.__class__._model.copy()
        self._local_model[name] = v

    def _get(self, name):
        if hasattr(self, "_local_model"):
            return self._local_model[name]
        return self.__class__._model[name]
 

class Test(TestModel):

    @field(pk=True)
    def name(self):
        return ""

    @name.filter
    def name(v):
        if not type(v) is str:
            raise ValueError("")
        if len(v) > 20:
            # meaningless error, just for test
            raise RuntimeError("")

        return v

    @field()
    def age(self):
        return 10
    
    @age.filter
    def age(v):
        if v > 99:
            raise ValueError("")

        return v

class TestDbField(unittest.TestCase):

    def test_basic(self):
        """
        basic usage of fields
        """
        m1 = Test()
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