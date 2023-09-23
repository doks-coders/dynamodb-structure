
class PassException(Exception):
    def __init__(self, exception=''):
        super(PassException, self) \
            .__init__('PassException: ' + str(exception))

class AFT: # 'ATF' stands for 'ATTR_FIELD_TRANSFORMAITONS'
    ###################################
    # GENERIC HELPER FUNCTIONS
    def DoObjectMapping(object, function, newFieldName=None, fieldName=None):
        try:
            if newFieldName is None and fieldName is None:
                print('Variant 1')
                function(object)
            if fieldName is None:
                object[newFieldName] = function(object)
            else:
                print('Variant 3')
                if isinstance(fieldName, dict):
                    raise NotImplementedError
                if fieldName in object:
                    object[newFieldName] = function(object, fieldName)
                
        except KeyError:
            pass
        except PassException:
            pass
        except Exception as e:
            pass
    # END GENERIC HELPER FUNCTIONS
    #################################################

    def ADD_SUFFIX_TO_PHONE(obj, element):
        phone_number =  obj[element] 
        return phone_number+' +234'

    def CREATE_FULLNAME(obj):
        return f"{obj['first_name']} {obj['last_name']}"
        

 