#_*_coding:utf-8_*_
import json
from elasticsearch import Elasticsearch


fb_xnr_index_name = 'fb_xnr'
fb_xnr_index_type = 'user'
ES_CLUSTER_HOST_2 = ['192.168.169.37:9206','192.168.169.38:9206']
# es_xnr_2 = es

task_id_list = ['FXNR0009','FXNR0001']
es_xnr_2 = Elasticsearch(ES_CLUSTER_HOST_2, timeout=600)

i = 1
for one_id in task_id_list:
    print 'update the {} user info'.format(i)   
    i = i + 1 
    item_exist = es_xnr_2.get(index=fb_xnr_index_name,doc_type=fb_xnr_index_type,id=one_id)['_source']
    item_exist['age'] = 25000000
    print '======================================='
    print es_xnr_2.update(index=fb_xnr_index_name,doc_type=fb_xnr_index_type,id=one_id,body={'doc':item_exist})

#query = {'query': {'match_all': {}}}
query = {'query': {'term':{'create_status':2}}}
allDoc = es_xnr_2.search(index=fb_xnr_index_name, doc_type=fb_xnr_index_type, body=query)
#print json.dumps(allDoc['hits']['hits'][0],ensure_asscii=False)
print len(allDoc['hits']['hits'])
for doc_info in allDoc['hits']['hits']:
    print json.dumps(doc_info,ensure_ascii=False)
    print '----------------------------------------------------------------------'
