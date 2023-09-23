from user import User
from business.request_data import RequestData

       
obj_array = [{
              'id':'ssnsv37382792jskjfsk',
              'first_name':'Daniel',
              'last_name':'Odokuma',
              'age':'20',
              'country':'Nigeria'
},
{
              'id':'370scnskn382983928',
              'first_name':'Jane',
              'last_name':'Mathews',
              'age':'22',
              'country':'America'
},
{
              'id':'32829948201002-1vsivsow',
              'first_name':'Peter',
              'last_name':'Dinklage',
              'age':'22',
              'country':'Canada'
},
{
              'id':'yuycwucu938923937iucsisi',
              'first_name':'Francis',
              'last_name':'Malverhine',
              'age':'22',
              'country':'Australia'
}]

obj = obj_array[0]
query={'first_name':'Jane','age': '22'}


# User.dynamoFind(RequestData.constructorOrgID('ngXdL'),query,limit=1,indexField="age",include=['last_name']) #Pass in the field you want to index, not name of index
# User.getOne(RequestData.constructorOrgID('ngXdL'),'ssnsv37382792jskjfsk')
# User.getAll(RequestData.constructorOrgID('ngXdL'),limit=1000)
# User.saveOne(RequestData.constructorOrgID('ngXdL'),obj)
# User.getFromIDs(RequestData.constructorOrgID('ngXdL'),['ssnsv37382792jskjfsk']) 










