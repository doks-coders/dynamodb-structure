import boto3
from boto3.dynamodb.types import TypeDeserializer,TypeSerializer


# REQUEST DATA
from business.request_data import RequestData
from BASE.base_dynamo import BASE_DYNAMO
# BASE_ELASTIC, BASE_DYNAMO

class BASE_DB(BASE_DYNAMO):

    # OVERRIDE ##############
    @classmethod
    def doElastic(cls):
        cls.doMongoFind
        return False
    @classmethod
    def getName(cls):
        raise NotImplementedError
    @classmethod
    def getMongoMapping(cls):
        raise NotImplementedError
    @classmethod
    def getMongoIndexes(cls):
        raise NotImplementedError
    @classmethod
    def getElasticIndexDefintion(cls):
        raise NotImplementedError
    #################

    @classmethod
    def saveMultiple(cls, rq, obj_list, fields=None):

        # Mongo Save Multiple
        cls.mongoSaveMultiple(rq, obj_list=obj_list)
        # Ffrom man
     


    @classmethod
    def saveOne(cls, rq: RequestData, obj, on_combine_function=None):
        if isinstance(rq, str):
            org_id = rq
            rq = RequestData()
            rq.setOrgId(org_id)

        if on_combine_function is not None:
            raise NotImplementedError
        #obj = cls._toOne(obj)
        return cls.dynamoSaveOne(rq,obj)
        


    @classmethod
    def getOne(cls, rq: RequestData, id):
        obj = cls.dynamoGetOne(rq,id)
        return obj
    
    @classmethod
    def getFromIDs(cls, rq, id_list):
        return cls.dynamoGetFromIDs(rq, id_list)
    
    @classmethod
    def getAll(cls, rq, limit=10000):
        query={}
        results_list = cls.dynamoFind(rq,query,limit)
        return results_list
    
  

    

    @classmethod
    def dynamodb_to_dict(cls, item):
        deserializer = TypeDeserializer()
        new_item = {}
        for k, v in item.items():
            new_item[k] = deserializer.deserialize(v)
        return new_item

    @classmethod
    def dict_to_dynamodb(cls, item):
        serializer = TypeSerializer()
        return {k: serializer.serialize(v) for k, v in item.items()}
    
    @classmethod
    def deleteOne(cls, rq, id):
        cls.dynamoDeleteOne(rq, id)


    


    
    # @classmethod
    # def addUpdate(cls, rq:RequestData, obj, on_combine_function=None):   
    #     if on_combine_function is not None:
    #         on_combine_function(obj)

    #     results_list = cls.saveOne(rq,obj)
    #     return results_list
    
    # @classmethod
    # def deleteItem(cls,rq:RequestData, id):
    #     query = {}
    #     query['_id'] = id
    #     result = cls.deleteOne(rq,query)
    #     return result
    
   
        
    #::END






