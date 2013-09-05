'''
Created on Aug 8, 2013

@author: Mission Liao
'''


import unittest

TEST_MODULES = [
    'toresdo.test.dal.basic',
    'toresdo.test.dal.mongo.motor',
    'toresdo.test.dal.sql.sqlite'
]


if __name__ == "__main__":
    suite = unittest.defaultTestLoader.loadTestsFromNames(TEST_MODULES)
    unittest.TextTestRunner(verbosity=2).run(suite)