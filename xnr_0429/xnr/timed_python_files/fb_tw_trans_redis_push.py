# -*- coding: utf-8 -*-
import redis

REDIS_HOST_37 = '192.168.169.37'
REDIS_PORT_37 = '6379'
r_37 = redis.StrictRedis(host=REDIS_HOST_37, port=REDIS_PORT_37)
r = r_37

r_ali = redis.Redis(host='60.205.190.67', port=6379, db=0)

batch = 3000

# redis_task = 'twitter_flow_text_trans_task'

def load_batch_data(redis_task):
    redis_task_temp = redis_task + '_ali'
    length = r.llen(redis_task)
    temp = r.lrange(redis_task, length-batch, length-1)
    if temp:
        for t in temp:
            r_ali.rpush(redis_task_temp, eval(t))
        return redis_task_temp
    return False

# def remove_batch_data(redis_task, length):
#     redis_task_temp = redis_task + '_temp'
#     for i in range(length):
#         r.rpop(redis_task)
#     r.delete(redis_task_temp)

if __name__ == '__main__':
    load_batch_data('twitter_flow_text_trans_task')
    load_batch_data('facebook_flow_text_trans_task')
    load_batch_data('facebook_user_trans_task')
    load_batch_data('twitter_user_trans_task')


