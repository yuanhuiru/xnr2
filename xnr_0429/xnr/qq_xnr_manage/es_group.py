# -*- coding: utf-8 -*-
from elasticsearch import Elasticsearch
import json

es_xnr = Elasticsearch(['192.168.169.45:9205', '192.168.169.47:9205', '192.168.169.47:9206'], timeout=600)

query_body = {
	'query':{
		'term':{'submitter':'admin@qq.com'}
	},
	'size': 9999
}

query_body = {
        "query": {
            "filtered":{
                "filter":{
                    "bool":{
                        "must":[
                            #{"term":{"xnr_qq_number":80617252}},
                            #{'terms':{'qq_group_nickname':['xnr专用测试群']}}

                        ]
                    }
                }
            }
            },
            "size": 100,
            "sort":{"timestamp":{"order":"desc"}}
        }


qq_xnr_index_type = 'user'
group_message_index_type = 'record'

result = es_xnr.search(index='group_message_2018-12-19', doc_type=group_message_index_type, body=query_body)['hits']['hits']

print json.dumps(result,ensure_ascii=False)

