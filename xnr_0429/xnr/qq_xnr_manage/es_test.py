# -*- coding: utf-8 -*-
from elasticsearch import Elasticsearch
import json

es_xnr = Elasticsearch('192.168.169.45:9205', timeout=600)

query_body = {
	'query':{
		'term':{'submitter':'xuanhui@qq.com'}
	},
	'size': 9999
}

qq_xnr_index_type = 'user'

result = es_xnr.search(index='qq_xnr', doc_type=qq_xnr_index_type, body=query_body)['hits']['hits']

print json.dumps(result,ensure_ascii=False)

