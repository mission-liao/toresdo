'''
Created on Sep 2, 2013

@author: Mission Liao
'''

import unittest
from toresdo.om.sql.sqlite import Model
from toresdo.om import field
from toresdo.om import Cond

class User(Model):
    
    @field()
    def name(self):
        return ""
    
    @field()
    def email(self):
        return ""
    
    @field()
    def age(self):
        return 0
    
    @field()
    def relation(self):
        return int

class TestDB_sqlite(unittest.TestCase):
    def test_where_clause(self):
        # A very basic one
        stmt = Cond.to_cmd(User, User.name == "Tom")
        self.assertEqual(stmt[0], "SELECT * FROM User WHERE name=?")
        self.assertEqual(stmt[1], ["Tom"])

        # combine with several boolean operator
        stmt = Cond.to_cmd(User, Cond.group(Cond.or__, User.name != "Tom", Cond.group(Cond.and__, User.age > 19, User.relation == 1)))
        self.assertEqual(stmt[0], "SELECT * FROM User WHERE (name<>? OR (age>? AND relation=?))")
        self.assertEqual(stmt[1], ["Tom", 19, 1])
        
        # boolean operator with more than 2 conditions
        stmt = Cond.to_cmd(User, Cond.group(Cond.or__, User.name == "Tom", User.name == "Mary", User.name == "Gibby"))
        self.assertEqual(stmt[0], "SELECT * FROM User WHERE (name=? OR name=? OR name=?)")
        self.assertEqual(stmt[1], ["Tom", "Mary", "Gibby"])
