'''
Created on Sep 2, 2013

@author: Mission Liao
'''

import unittest
from toresdo.dal.sql.sqlite import Model
from toresdo.dal import field
from toresdo.dal import Cond

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
    
    @field()
    def address(self):
        return str

class TestDB_sqlite(unittest.TestCase):
    def tearDown(self):
        Model.release()

    def test_where_clause(self):
        # A very basic one
        stmt = Cond.to_cmd(User, User.name == "Tom")
        self.assertEqual(stmt[0], "SELECT * FROM User WHERE name=?")
        self.assertEqual(stmt[1], ["Tom"])

        # combine with several boolean operator
        stmt = Cond.to_cmd(User, Cond.group(Cond.or__,
                                            User.name != "Tom",
                                            Cond.group(Cond.and__,
                                                       User.age > 19,
                                                       User.relation == 1)))
        self.assertEqual(stmt[0], "SELECT * FROM User WHERE (name<>? OR (age>? AND relation=?))")
        self.assertEqual(stmt[1], ["Tom", 19, 1])

        # boolean operator with more than 2 conditions
        stmt = Cond.to_cmd(User, Cond.group(Cond.or__, User.name == "Tom", User.name == "Mary", User.name == "Gibby"))
        self.assertEqual(stmt[0], "SELECT * FROM User WHERE (name=? OR name=? OR name=?)")
        self.assertEqual(stmt[1], ["Tom", "Mary", "Gibby"])
        
        # more complex one, with shuffle group-condition and single-condition
        stmt = Cond.to_cmd(User,
                           Cond.group(Cond.and__,
                                      User.name == "Tom",
                                      Cond.group(Cond.or__,
                                                 User.age > 19,
                                                 User.relation == 1),
                                      User.name == "Mary",
                                      Cond.group(Cond.or__,
                                                 User.name == "Jeff",
                                                 User.name == "Bezos"),
                                      User.name == "Qoo"))
        self.assertEqual(stmt[0], "SELECT * FROM User WHERE (name=? AND (age>? OR relation=?) AND name=? AND (name=? OR name=?) AND name=?)")
        self.assertEqual(stmt[1], ["Tom", 19, 1, "Mary", "Jeff", "Bezos", "Qoo"])
        
    def test_insert_and_select(self):
        # insert a new user
        u = User()
        u.name = "Tom"
        u.email = "tom@hotmail.com"
        u.age = 19
        u.relation = 1
        u.save()

        # query it back
        uu = User.find_one(User.name == "Tom")
        self.assertEqual(uu.name, "Tom")
        self.assertEqual(uu.email, "tom@hotmail.com")
        self.assertEqual(uu.age, 19)
        self.assertEqual(uu.relation, 1)

    def test_select_more_than_one(self):
        u = User()
        u.name = "Tom"
        u.email = "tom@hotmail.com"
        u.age = 19
        u.relation = 1
        u.save()

        u = User()
        u.name = "Mary"
        u.age = 19
        u.relation = 0
        u.save()
        
        # query for both Tom and Mary
        b_tom = False
        b_mary = False
        for uu in User.find(User.age < 20):
            if uu.name == "Tom":
                b_tom = True
            elif uu.name == "Mary":
                b_mary = True
                self.assertEqual(uu.age, 19)
                self.assertEqual(uu.relation, 0)
                self.assertEqual(uu.email, "")
    
        self.assertTrue(b_tom)
        self.assertTrue(b_mary)
        
    def test_init_with_kwargs(self):
        u = User(name="Tom", email="a@a.com", age=19)
        self.assertEqual(u.name, "Tom")
        self.assertEqual(u.email, "a@a.com")
        self.assertEqual(u.age, 19)