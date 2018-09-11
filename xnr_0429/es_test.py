# -*- coding: utf-8 -*-
from  elasticsearch import Elasticsearch
import json

facebook_user_index_name = 'facebook_user'
facebook_user_index_type = 'user'
ES_CLUSTER_HOST_2 = ['192.168.169.37:9206','192.168.169.38:9206']
es_xnr_2 = Elasticsearch(ES_CLUSTER_HOST_2, timeout=600)



q = {
    'query':{
        'match_all': {}
    },
    'size': 99
}

res = es_xnr_2.search(facebook_user_index_name, facebook_user_index_type, q)['hits']['hits']
for i in res:
	print 111111111111111111111111
	print json.dumps(i,ensure_ascii=False)
