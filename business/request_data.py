
import time
import datetime
import re
from http import cookies
import base64
import traceback
from urllib.parse import parse_qs
import json
#import pymongo




class rest_attr(object):
    PATH_INFO               = 'PATH_INFO'
    REQUEST_METHOD          = 'REQUEST_METHOD'
    CONTENT_LENGTH          = 'CONTENT_LENGTH'
    QUERY_STRING            = 'QUERY_STRING'
    HTTP_HOST               = 'HTTP_HOST'
    SERVER_NAME             = 'SERVER_NAME'
    REMOTE_ADDR             = 'REMOTE_ADDR'
    HTTP_X_FORWARDED_FOR    = 'HTTP_X_FORWARDED_FOR'

class methods(object):
    GET                     = 'GET'
    POST                    = 'POST'
    PUT                     = 'PUT'
    DELETE                  = 'DELETE'
    OPTIONS                 = 'OPTIONS'

DO_MONGO = True

class RequestData:
    # CONSTRUCTORS
    def __init__(self):
        self._request      = None
        self._path_list    = None
        self._query_args   = None
        self.post_data     = None
        self._method       = None
        self._org_id       = None
        self._current_user = None
        self._subdomain    = None
        self.name          = 'ngXdL'  

        self._org_details  = None

        self._cache = {}
        self._cache_in_progress = {}

        self._sets   = {}
        self._counts = {}

        self._values = {}

        self._brand_cache = {}
        self._people_cache = {}

        self.nord_server_map      = None
        self.nord_server_map_time = None

        self._response_headers = []

        self.documentDBClient = None
        self.documentDBServer = None

        # RELAY SERVER
        self.CACHE = {}
        self.examineDB = False
        
    
    @classmethod
    def constructorOrgID(cls, org_id):
        rq = cls()
        rq.setOrgId(org_id)
        return rq


    # Environ
       
    
    def setRequest(self, in_request):
        self._request = in_request

    def getRequest(self):
        return self._request
    # Get Query Arg
    def getQueryArg(self, key, default=None):
        if self._query_args is None:
            self._query_args = parse_qs(self._environ[rest_attr.QUERY_STRING])
        try:
            return self._query_args[key][0]
        except:
            return default
    def get_query_arg(self, key, default=None):
        return self.getQueryArg(key, default)
  
   
    def getPathList(self):
        return self._path_list

    # Method
    # def setMethod(self, in_method):
    #     self._method = in_method
    def getMethod(self):
        return self._method

    # Organizarion
    def getOrganization(self):
        return self.organization_obj

    # OrIg
    def setOrgId(self, in_org_id):
        self._org_id = in_org_id
    def getOrgId(self):
        return self._org_id

    # Current User
   

    