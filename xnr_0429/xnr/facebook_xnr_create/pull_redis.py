
#_*_coding:utf-8_*_

import json
import redis
from elasticsearch import Elasticsearch

def get_result_from_redis():
    fb_xnr_index_name = 'fb_xnr'
    fb_xnr_index_type = 'user'
    ES_CLUSTER_HOST_2 = ['192.168.169.37:9206','192.168.169.38:9206']
    es_xnr_2 = Elasticsearch(ES_CLUSTER_HOST_2, timeout=600)

    cl = redis.Redis(host='60.205.190.67', port=6379, db=0)


    result_info1 = cl.rpop('result_info')
    result_info = cl.rpop('params')


    print 'pop info'
    print result_info
while 1:
    get_result_from_redis()
