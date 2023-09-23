import copy

class BASE_PARENT:

    ########################
    # ID CONVERSTION - THESE METHODS CONVERT 'id' to "_id"
    @classmethod
    def _fromOne(cls, obj):
        obj['id'] = obj.pop('_id')
        return obj
    @classmethod
    def _fromCollection(cls, obj_list):
        for obj in obj_list:
            cls._fromOne(obj)
        return obj_list
    @classmethod
    def _toOne(cls, obj):
        obj = copy.deepcopy(obj)
        obj['_id'] = obj.pop('id')
        return obj
    @classmethod
    def _toMany(cls, obj_list):
        for i in range(len(obj_list)):
            obj_list[i]= cls._toOne(obj_list[i])
        return obj_list
    @classmethod
    def _fromMany(cls, obj_list):
        for i in range(len(obj_list)):
            obj_list[i]= cls._fromOne(obj_list[i])
        return obj_list
    ### END ID CONVERSATION METHODS