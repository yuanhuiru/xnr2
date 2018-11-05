import redis
import json


# 60.205.190.67

# 47.94.133.29
cl = redis.Redis(host='47.94.133.29', port=9506, db=0)
operate_info = {}
operate_info['task_id'] = 1
operate_info['task_info'] = 'execute spider 1'
operate_info['task_id'] = 2
operate_info['task_info'] = 'execute spider 2'

try:
	content = cl.lpush('operate', json.dumps(operate_info))
except Exception as e:
	print e

print('successful end')
print cl.lrange('operate', 0, 10)

