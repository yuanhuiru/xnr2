#_*_coding:utf-8_*_
import json
from elasticsearch import Elasticsearch


fb_xnr_index_name = 'fb_xnr'
fb_xnr_index_type = 'user'
ES_CLUSTER_HOST_2 = ['192.168.169.37:9206','192.168.169.38:9206']
# es_xnr_2 = es

task_id = 'FXNR0009'
es_xnr_2 = Elasticsearch(ES_CLUSTER_HOST_2, timeout=600)



#query = {'query': {'match_all': {}}}
query = {'query': {'term':{'create_status':2}}}
allDoc = es_xnr_2.search(index=fb_xnr_index_name, doc_type=fb_xnr_index_type, body=query)
#print json.dumps(allDoc['hits']['hits'][0],ensure_asscii=False)
print len(allDoc['hits']['hits'])
for doc_info in allDoc['hits']['hits']:
    print json.dumps(doc_info,ensure_ascii=False)
    print '----------------------------------------------------------------------'
