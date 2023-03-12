# keepvariable
A Python package keeping the values of variables between separate runs in a seamless and effortless way.

## Installation

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install keepvariable.

```bash
pip install keepvariable
```

## Usage with Redis

```python
import keepvariable_core as kv
import pandas as pd

kv_redis=kv.KeepVariableRedisServer(host="127.0.0.1",port=6379)
    

kv_redis.set("test","abc123")
result=kv_redis.get("test")

print(result) 
#abc123



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
```

## Usage (locally)

```python
from keepvariable_core import Var,kept_variables,save_variables,load_variable

a=Var("b")
b=Var("c")

c=a+b

print(c)

dict1=Var({1,2,3,4,5})

list1=Var(a)

print(kept_variables)
save_variables(kept_variables)

list1=load_variable()

b=load_variable()

c=Var(c)
print(list1)
```
![obrazek](https://user-images.githubusercontent.com/29150831/224581261-8f3c6d10-445c-440d-bec9-0b9645a01cd0.png)



## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

## License
[MIT](https://choosealicense.com/licenses/mit/)
