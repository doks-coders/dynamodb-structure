import boto3
from BASE.base_parent import BASE_PARENT
from business.request_data import RequestData
from config.credentials import DYNAMODB
from boto3.dynamodb.types import TypeDeserializer, TypeSerializer
import datetime
import traceback
import botocore
# NOTE-CACHE the dynamodb


def get_client():
    try:
        dynamodb = boto3.resource('dynamodb',
                                  endpoint_url=DYNAMODB.endpoint_url,
                                  region_name=DYNAMODB.region_name,  # This can be any valid AWS region
                                  # Use 'dummy' or any other placeholder text
                                  aws_access_key_id=DYNAMODB.aws_access_key_id,
                                  aws_secret_access_key=DYNAMODB.aws_secret_access_key,  #
                                  )
        return dynamodb
    except Exception as e:
        raise e


class BASE_DYNAMO(BASE_PARENT):

    #####################################
    # OVERRIDE THESE
    @classmethod
    def getName(cls):
        raise NotImplementedError
    #####################################

    @classmethod
    def _getClient(cls):
        # AWS provides the endpoint (url) for your DocumentDB cluster
        try:
            client = get_client()
            return client
        except Exception as e:
            raise e
    # ::END

    @classmethod
    def _getOrg(cls, requestData: RequestData):
        # Get the client
        if isinstance(requestData, str):
            org_id = requestData
            requestData = RequestData()
            requestData.setOrgId(org_id)

        # Specify the database to be used
        db_name = 'boogie_'+requestData.getOrgId()
        return db_name
    # ::END

    @classmethod
    def _getPayload(cls, obj):
        from BASE.aft import AFT
        instructionList = cls.getInstructionsForTheIndexFields()
        for instruction in instructionList:
            try:
                if type(instruction[1]) == tuple:
                    AFT.DoObjectMapping(obj, instruction[0], *instruction[1])
                else:

                    AFT.DoObjectMapping(obj, instruction[0], instruction[1])
            except Exception as e:
                raise e

        obj['last_updated'] = datetime.datetime.utcnow().isoformat()
        payload = obj
        return payload

    @classmethod
    def globalSecondaryIndexName(cls, field):
        return f'Org{field.capitalize()}'
    #::END

    @classmethod
    def getGlobalSecondaryIndex(cls): #Gets the indexes from the Dataobject and
        indexes = cls.getDynamoIndexes()
        GlobalSecondaryIndex = []
        for (field, type) in indexes:
            obj = {}
            keySchema = []
            index_name = cls.globalSecondaryIndexName(field)
            obj['IndexName'] = index_name
            keySchema.append({'AttributeName': 'org_id', 'KeyType': 'HASH'})
            keySchema.append({'AttributeName': field, 'KeyType': 'RANGE'})
            obj['KeySchema'] = keySchema
            obj['Projection'] = {'ProjectionType': 'ALL'}
            obj['ProvisionedThroughput'] = {
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5,
            }
            GlobalSecondaryIndex.append(obj)
        return GlobalSecondaryIndex
    #::END

    @classmethod
    def getIndexAttributeDefinitions(cls):
        indexes = cls.getDynamoIndexes()
        IndexAttributeDefinitions = []
        for field, type in indexes:
            IndexAttributeDefinitions.append(
                {
                    'AttributeName': field,
                    'AttributeType': type
                },
            )
        return IndexAttributeDefinitions
    #::END

    @classmethod
    def createDynamoTable(cls):
        try:
            table_name = cls.getName()
            dynamodb = cls._getClient()
            table_args = {
                'TableName': table_name,
                'KeySchema': [
                    {
                        'AttributeName': 'org_id',
                        'KeyType': 'HASH'  # Partition key
                    },
                    {
                        'AttributeName': 'id',
                        'KeyType': 'RANGE'  # Partition key
                    },
                ],
                'AttributeDefinitions': [
                    {
                        'AttributeName': 'org_id',
                        'AttributeType': 'S'
                    },
                    {
                        'AttributeName': 'id',
                        'AttributeType': 'S'
                    },
                ] + cls.getIndexAttributeDefinitions(),
                'ProvisionedThroughput': {
                    'ReadCapacityUnits': 5,
                    'WriteCapacityUnits': 5
                },

            }
            if len(cls.getGlobalSecondaryIndex()) > 0:
                table_args['GlobalSecondaryIndexes'] = cls.getGlobalSecondaryIndex()

            response = dynamodb.create_table(
                **table_args
            )
            return response
        except Exception as e:
            print('This was the error: ', e)
            return None
    # ::END

    @classmethod
    def _getDynamoTable(cls):
        # Get the client
        dynamodb = cls._getClient()
        existing_tables = dynamodb.meta.client.list_tables()['TableNames']
        if cls.getName() not in existing_tables: #Check if Table Exists
            cls.createDynamoTable()
        table = dynamodb.Table(cls.getName())

        return table
    # ::END

    @classmethod
    def addOrg(cls, rq: RequestData, obj):
        org_id = cls._getOrg(rq)
        obj['org_id'] = org_id
        return obj
    # ::END

    @classmethod
    def dynamoSaveOne(cls, rq: RequestData, obj):
        # Get the connection to the database
        obj = cls._getPayload(obj)
        # obj = cls.dict_to_dynamodb(obj)
        database = cls._getDynamoTable()
        obj = cls.addOrg(rq, obj)
        is_new = True
        try:
            result = database.put_item(
            Item=obj,
            ConditionExpression='attribute_not_exists(id)',
        )
            print("Successfully added new item.")
            return result
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                is_new=False
                print("Item with the same primary key already exists.")
            else:
                print("Unexpected error: %s" % e)

        return is_new, obj
    #::END
        
   
    
    @classmethod
    def getConditionExpression(cls, query):
        org_value = query.pop('org_id')  # The Organisation is always there
        # Constant Key Condition Expressions
        KeyConditionExpression = 'org_id = :org_id_value'

        id_value = None
        if query.get('id'):  # Checks If there is an id in the query
            id_value = query.pop('id')
            KeyConditionExpression += ' AND id = :id_value'

        # After removing the key expressions from the query, we can use the rest of the query as filter expressions
        FilterExpression = ''
        ExpressionAttributeValues = {}
        keys = list(query.keys())
      
        filter_parts = [f"{key} = :value{i+1}"  for i, key in enumerate(keys)]

        FilterExpression = " AND ".join(filter_parts)
        [f"{key} = :value{i+1}"  for i, key in enumerate(keys)]

        for i in range(len(keys)): #placeholder filters , will be :value1, :value2 ... and so on
            ExpressionAttributeValues[f':value{i+1}'] = query[keys[i]]
        

        if id_value:
            ExpressionAttributeValues[':id_value'] = id_value

        ExpressionAttributeValues[':org_id_value'] = org_value
        return (
            ExpressionAttributeValues,
            FilterExpression,
            KeyConditionExpression
        )
    #::END

    @classmethod
    def getIndexConditionExpressions(cls,rq, indexName,query): #Takes the indexName and checks the indexes of the table to find the fields that need to be indexed
        table = cls._getDynamoTable()
        query = cls.addOrg(rq,query)
        global_secondary_indexes= table.global_secondary_indexes
        KeyConditionExpression = ''
        ExpressionAttributeValues ={}
        for global_index in global_secondary_indexes:
            if global_index['IndexName'] ==indexName:
                keySchema =global_index['KeySchema']
                i=0
                for key in keySchema:
                    KeyConditionExpression += f"{' AND 'if(i>0) else ''}{key['AttributeName']} = :{key['AttributeName']}_value"
                    ExpressionAttributeValues[f":{key['AttributeName']}_value"] = query[key['AttributeName']]
                    query.pop(key['AttributeName'])
                    i+=1

        keys = list(query.keys())
        filter_parts = [f"{key} = :value{i+1}"  for i, key in enumerate(keys)]
        FilterExpression = " AND ".join(filter_parts)
        [f"{key} = :value{i+1}"  for i, key in enumerate(keys)]

        for i in range(len(keys)): #placeholder filters , will be :value1, :value2 ... and so on
            ExpressionAttributeValues[f':value{i+1}'] = query[keys[i]]

        return(KeyConditionExpression,ExpressionAttributeValues,FilterExpression)
    #::END

    @classmethod
    def dynamoFind(cls, rq: RequestData, query, limit=1000, include=None, exclude=None, projection=None, sort=None,indexField=None):
        indexName=None
        if indexField:
            indexName = cls.globalSecondaryIndexName(indexField)
        
        table = cls._getDynamoTable()
        query = cls.addOrg(rq, query)
        query_args = {"Limit": limit}

        if not indexName:
              #Get Expressions From query
            (ExpressionAttributeValues, FilterExpression, KeyConditionExpression
            ) = cls.getConditionExpression(query)
            query_args["KeyConditionExpression"]= KeyConditionExpression
            query_args ["ExpressionAttributeValues"]= ExpressionAttributeValues
            if FilterExpression != '':
                query_args['FilterExpression'] = FilterExpression
        else:
            # Get Indexed Expressions From query
           (KeyConditionExpression,ExpressionAttributeValues,FilterExpression
         )= cls.getIndexConditionExpressions(rq,indexName,query)
           query_args['IndexName'] = indexName
           query_args['KeyConditionExpression'] = KeyConditionExpression
           query_args['ExpressionAttributeValues']=ExpressionAttributeValues
           if FilterExpression != '':
                query_args['FilterExpression'] = FilterExpression
        
        
        if include:
            if len(include)>0:
                query_args['ProjectionExpression'] = ", ".join(include)

        response = table.query(**query_args)
        result = response['Items']
        return result
    #::END

    @classmethod
    def dynamoGetOne(cls, rq: RequestData, id):
        query = {'id':  str(id)}
        doc_list = cls.dynamoFind(rq, query)
        if len(doc_list) >= 1:
            return doc_list[0]
        return None
    #::END

    @classmethod
    def dynamoDeleteOne(cls, rq:RequestData, id):
        table = cls._getDynamoTable()
        key = {
            'id': id,
            'org_id':cls._getOrg(rq)
        }
        response = table.delete_item(
        Key=key
        )
        print(response)
    #::END

    @classmethod
    def dynamoGetFromIDs(cls,rq,id_list):
        table = cls._getDynamoTable()
        # List of IDs to get

        # Construct keys
        keys = [{'id': i,'org_id':cls._getOrg(rq)} for i in id_list]

        # Use batch_get_item to get multiple items by id
        response = table.meta.client.batch_get_item(
                RequestItems={
                    cls.getName(): {
                        'Keys': keys
                    }
                }
            )

            # Access the returned items
        items = response['Responses'][cls.getName()]

            # Print or process the items
        return items

        


   













    '''
             
  "KeyConditionExpression": KeyConditionExpression,
            "ExpressionAttributeValues": ExpressionAttributeValues,
            "Limit": limit,
if FilterExpression != '':
            query_args['FilterExpression'] = FilterExpression
        if indexName:
            query_args['IndexName'] = indexName

 {   

                'IndexName':'OrgEmployee_id',
                'KeyConditionExpression':'org_id = :org_id_value AND employee_id = :employee_id_value',
                'ExpressionAttributeValues':{
                ':employee_id_value': '2022-01-01T10:00:00',
                ':org_id_value': 'boogie_ngXdL'
            }
       
        '''