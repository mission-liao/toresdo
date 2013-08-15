'''
Created on Aug 5, 2013

@author: Mission Liao
'''

from __future__ import absolute_import


import toresdo.db.field

class ModelBase(object):

    def __init__(self):
        if hasattr(self.__class__, "_model_prepare_cookie") == False or self.__class__._model_prepare_cookie == None:
            if hasattr(self.__class__, "_prepare"):
                """
                generate a dict of field
                """
                fields = {}
                for k,v in self.__class__.__dict__.items():
                    if type(v) == toresdo.db.field:
                        fields.update({k: v})

                # TODO: cache exception
                self.__class__._prepare(fields)
                self.__class__._model_prepare_cookie = True

            else:
                """
                this model didn't need preparation, so we just need to
                assign any non-None value to stop reentry to _prepare
                """
                self.__class__._model_prepare_cookie = True

    