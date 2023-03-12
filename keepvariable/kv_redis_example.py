import keepvariable.keepvariable_core as kv


kv_redis=kv.KeepVariableRedisServer(host="127.0.0.1",port=6379)
    
kv_redis.set("test","abc123")
result=kv_redis.get("test")

print(result) #abc123