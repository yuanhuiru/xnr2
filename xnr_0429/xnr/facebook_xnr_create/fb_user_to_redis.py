#_*_coding:utf-8_*_
import json
import redis
import sys
from elasticsearch import Elasticsearch
sys.path.append('/home/xnr1/xnr_0429/xnr/')
from global_utils import es_xnr_2

fb_xnr_index_name = 'fb_xnr'
fb_xnr_index_type = 'user'
# ES_CLUSTER_HOST_2 = ['192.168.169.37:9206','192.168.169.38:9206']
# es_xnr_2 = Elasticsearch(ES_CLUSTER_HOST_2, timeout=600)

cl = redis.Redis(host='60.205.190.67', port=6379, db=0)


query = {'query': {'term':{'create_status':2}}}
allDoc = es_xnr_2.search(index=fb_xnr_index_name, doc_type=fb_xnr_index_type, body=query)

task_redis_key = 'facebook_params'

for doc_info in allDoc['hits']['hits']:
    params_dict = {}
    doc_info_str =  json.dumps(doc_info,ensure_ascii=False)
    doc_info_dict =  json.loads(doc_info_str)

    params_dict['xnr_id'] = doc_info_dict['_id']
    if doc_info_dict['_source']['fb_phone_account'] == '':
        params_dict['user_account'] = doc_info_dict['_source']['fb_mail_account']
    params_dict['user_account'] = doc_info_dict['_source']['fb_phone_account']
    params_dict['user_password'] = doc_info_dict['_source']['password']
    print params_dict
    # 放入redis队列
    try:
        content = cl.lpush(task_redis_key, json.dumps(params_dict))
    except Exception as e:
        print e

    #print json.dumps(doc_info,ensure_ascii=False)['nick_name']
    print content, 'already push to aliyun redis !!!!!!----------------------------------------------------------------------'

print cl.lrange('params', 0, 10)

