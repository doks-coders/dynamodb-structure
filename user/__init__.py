
# PERSON
# DATALAYER
from BASE import BASE_DB
from BASE.aft import AFT
from business.request_data import RequestData




class User(BASE_DB):

        ### OVERRIDES ###
        @classmethod
        def getName(cls):
                return 'user'
        #    MONGO
        @classmethod
        def getInstructionsForTheIndexFields(cls):
            return [
                  (AFT.CREATE_FULLNAME,      ('i-fullname'))
                 ]  

        @classmethod
        def getDynamoIndexes(cls):
            return [
                     ('first_name','S'),
                     ('last_name','S'),
                     ('age','S'),
                     ('i-fullname','S')
                ]
 
        
        
        #################

        

