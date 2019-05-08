#_*_coding:utf-8_*_
import json
from elasticsearch import Elasticsearch


nick_name = 'xrodman'
es_xnr = Elasticsearch(['192.168.169.45:9205','192.168.169.46:9205','192.168.169.47:9205'], timeout=600)

# 查询表中所有
query_body = {'query': {'match_all': {}}}# 查找所有文档

#query_body = {
#        'query':{
#                    'term':{'nick_name':nick_name}
#                }
#        }
qq_xnr_index_name = 'qq_xnr'
qq_xnr_index_type = 'user'

all_qq_xnr_results = es_xnr.search(index=qq_xnr_index_name, doc_type=qq_xnr_index_type, body = query_body)

print json.dumps(all_qq_xnr_results,ensure_ascii=False)
