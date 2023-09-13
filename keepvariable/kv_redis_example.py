import keepvariable_core as kv
import pandas as pd
import datetime
import inspect

from credentials import REDIS_PASSWORD

#kv_redis=kv.KeepVariableRedisServer(host="app.forloop.ai",port=6379,password=REDIS_PASSWORD)
kv_redis=kv.KeepVariableDummyRedisServer()

    

kv_redis.set("test","abc123")
result=kv_redis.get("test")

print(result) 
#abc123


kv_redis.set("integer_test",1)
result=kv_redis.get("integer_test")
print(result) 
#1


kv_redis.set("float_test",1.5)
result=kv_redis.get("float_test")
print(result) 
#1.5




#even pandas dataframes, and numpy arrays can be stored
df=pd.DataFrame([[1,2,3,4],[4,5,6,7]],columns=["a","b","c","d"])    
array=df.values    

    
kv_redis.set("test_df",df)
result=kv_redis.get("test_df")

print(result)
#   a  b  c  d
#0  1  2  3  4
#1  4  5  6  7
    
kv_redis.set("test_array",array)
result=kv_redis.get("test_array")

print(result)
# #[[1 2 3 4]
# # [4 5 6 7]]

datetime_test = datetime.datetime(year=2023, month=4, day=15, hour=14, minute=35)

print(f'Datetime original: {datetime_test}')

name = "datetime_test"
kv_redis.set(name, datetime_test)
result = kv_redis.get(name)

print(f'Datetime from redis: {result}')
# 2023-04-14 14:35:00

def test_func():
    print('Hello world!')
name = "test_func"
codelines, _ = inspect.getsourcelines(test_func)
code = "".join(codelines)
kv_redis.set(name, test_func, {"code": code})
result = kv_redis.get(name)
print(f'Function from redis:\n {result}')
# returns the code of the function

class Dog:
    def __init__(self, number_of_legs:int = 8):
        self.number_of_legs = number_of_legs
        
name = "Dog"
codelines, _ = inspect.getsourcelines(Dog)
code = "".join(codelines)
kv_redis.set(name, Dog, {"code": code})
result = kv_redis.get(name)
print(f'Class from redis:\n {result}')
# returns the code of the class  



