#_*_coding:utf-8_*_
import json
from elasticsearch import Elasticsearch


nick_name = 'xrodman'
es_xnr = Elasticsearch(['192.168.169.45:9205','192.168.169.46:9205','192.168.169.47:9205'], timeout=600)


query_body = {
        'query':{
                    'term':{'nick_name':nick_name}
                }
        }

weibo_xnr_index_name = 'weibo_xnr'
weibo_xnr_index_type = 'user'
es_results = es_xnr.search(index=weibo_xnr_index_name,doc_type=weibo_xnr_index_type,body=query_body)['hits']['hits']


print es_results
