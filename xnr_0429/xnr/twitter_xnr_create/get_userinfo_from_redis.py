#_*_coding:utf-8_*_

import json
import redis
from elasticsearch import Elasticsearch

def get_result_from_redis():
    tw_xnr_index_name = 'tw_xnr'
    tw_xnr_index_type = 'user'
    ES_CLUSTER_HOST_2 = ['192.168.169.37:9206','192.168.169.38:9206']
    es_xnr_2 = Elasticsearch(ES_CLUSTER_HOST_2, timeout=600)

    cl = redis.Redis(host='60.205.190.67', port=6379, db=0)

    print cl.lrange('twitter_result_info', 0, 10)
    result_info = cl.rpop('twitter_result_info')
    print 'pop info'
    print result_info
    try:
        result_info_dict = json.loads(result_info) 
        task_id = result_info_dict['xnr_id']
    except Exception as e:
        return 0
    item_exist = es_xnr_2.get(index=tw_xnr_index_name,doc_type=tw_xnr_index_type,id=task_id)['_source']
    item_exist['description'] = result_info_dict['description']
    item_exist['location'] = result_info_dict['location']
    item_exist['id'] = result_info_dict['uid']
 
    print es_xnr_2.update(index=tw_xnr_index_name,doc_type=tw_xnr_index_type,id=task_id,body={'doc':item_exist})




if __name__ == '__main__':
    get_result_from_redis()

