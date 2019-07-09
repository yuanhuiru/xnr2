# -*- coding: utf-8 -*-
from elasticsearch import Elasticsearch


es_xnr = Elasticsearch(['192.168.169.37:9206'], timeout=600)

query_body = {
    'query':{
        'filtered':{
            'filter':{
                'term':{'task_status':0}
            }
        }
    },
    'size':999
}
results = es_xnr.search(index='tw_tweet_timing_list',doc_type='timing_list',body=query_body)['hits']['hits']


print results
# id_list = [item['_id'] for item in res]


#for id in id_list:
#    print es_xnr.delete(index='weibo_xnr_relations',doc_type='user',id=id)
