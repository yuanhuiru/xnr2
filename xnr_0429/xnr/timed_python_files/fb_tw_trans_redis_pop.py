# -*- coding: utf-8 -*-
import redis
import sys
sys.path.append('/home/xnr1/xnr_0429/xnr')
from global_utils import es_xnr_2 as es

r_ali = redis.Redis(host='60.205.190.67', port=6379, db=0)
r = r_ali

redis_task_es = 'redis_task_es'

while r_ali.llen(redis_task_es):
    data = r.rpop(redis_task_es)
    if data:
        data = eval(data)
        bulk_action = data['bulk_action']
        index_name = data['index_name']
        doc_type = data['doc_type']

        try:
            print es.bulk(bulk_action,index=index_name,doc_type=doc_type,timeout=600)
        except Exception,e: #如果出现错误，就减小存储的批次，再次出现问题的批次直接放弃
            # print 'my_bulk_func Exception: ', str(e)
            for i in range(len(bulk_action)/2):
                temp_bulk_action = bulk_action[2*i : 2*i+2]
                try:
                    es.bulk(temp_bulk_action,index=index_name,doc_type=doc_type,timeout=600)
                except:
                    pass
    else:
        break
