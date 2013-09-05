'''
Created on Aug 19, 2013

@author: Mission Liao
'''

import motor
import tornado.testing
from toresdo.om.mongo.motor import Model
from toresdo.om import field
from toresdo.om import Cond

class User(Model):
    
    __toresdo_db_conn__ = motor.MotorClient().open_sync()
    __toresdo_db_name__ = "db"
    
    @field()
    def name(self):
        return ""
    
    @field()
    def age(self):
        return 0
    
    @field()
    def email(self):
        return ""
    
    @field()
    def relation(self):
        return 0
    
    @field()
    def address(self):
        return ""


class TestDB_motor(tornado.testing.AsyncTestCase):
    def get_new_ioloop(self):
        """
        It seems motor/greenlet can't coexist with
        mulitple IOLoop. Therefore we just reference
        global IOLoop among test-cases of motor
        """
        return tornado.ioloop.IOLoop.instance()
    
    def test_with_id(self):
        """
        '_id' is a default field of mongodb
        """
        t = User()
        self.assertEqual(True, hasattr(t, "_id"))

    def test_with_save(self):
        """
        verify 'save' works.
        """
        def handle_save(result, err):
            self.assertEqual(err, None)
            self.stop()

        t = User()
        t.age = 19
        t.name = "Roy"
        t.save(callback=handle_save)
        self.wait(timeout=60)
        
    def test_query_stat(self):
        # A very basic one
        ctx = Cond.to_cmd(User, User.name == "Tom")
        self.assertEqual(ctx, {"name": "Tom"})

        # combine with several boolean operator
        ctx = Cond.to_cmd(User, Cond.group(Cond.or__, User.name != "Tom", User.name == "Mary", Cond.group(Cond.and__, User.age > 19, User.relation == 1, User.age < 5)))
        self.assertEqual(ctx,
                          {"$or": [{"name": {"$ne": "Tom"}},
                                  {"name": "Mary"}, 
                                  {"$and": [{"age": {"$gt": 19}},
                                            {"relation": 1},
                                            {"age": {"$lt": 5}}
                                            ]}]})

        # boolean operator with more than 2 conditions
        ctx = Cond.to_cmd(User, Cond.group(Cond.or__, User.name == "Tom", User.name == "Mary", User.name == "Gibby"))
        self.assertEqual(ctx, {"$or": [{"name": "Tom"},
                                       {"name": "Mary"},
                                       {"name": "Gibby"},
                                       ]})
