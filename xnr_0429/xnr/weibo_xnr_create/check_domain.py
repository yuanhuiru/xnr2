#_*_coding:utf-8_*_
import json
from elasticsearch import Elasticsearch


nick_name = 'xrodman'
es_xnr = Elasticsearch(['192.168.169.45:9205','192.168.169.46:9205','192.168.169.47:9205'], timeout=600)


weibo_domain_index_name = 'weibo_domain'
weibo_domain_index_type = 'group'


domain_name_dict = {}

query_body = {'query':{'term':{'compute_status':3}},'size':100}
es_results = es_xnr.search(index=weibo_domain_index_name,doc_type=weibo_domain_index_type,body=query_body)['hits']['hits']
if es_results:
    for result in es_results:
        result = result['_source']
        domain_name_dict[result['domain_pinyin']] = result['domain_name']

print es_results
