# -*-coding:utf-8-*-
import sys
import json
sys.path.append('../')
from global_utils import RE_QUEUE as re
from global_utils import R_OPERATE_QUEUE as r, operate_queue_name, FB_TWEET_PARAMS
# R_OPERATE_QUEUE = redis.StrictRedis(host=REDIS_HOST_45, port=REDIS_PORT_45, db=3) 
# operate_queue_name = 'opetate' 

#print re.lrange('params', 0, 10)
#print re.lrange('fb_tweet_params', 0, 10)
# 测试45redis上的队列
test_dict = {"test":"hello world"}
#r.lpush(operate_queue_name, json.dumps(test_dict))
#print r.lrange("operate", 0, 10)
print re.lrange(FB_TWEET_PARAMS, 0, 10)
#print re.lrange(operate_queue_name, 0, 10)

