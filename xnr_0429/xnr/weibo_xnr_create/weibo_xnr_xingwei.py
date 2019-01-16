#_*_coding:utf-8_*_
import json
from elasticsearch import Elasticsearch


xnr_user_no = 'WXNR0152'
es_xnr = Elasticsearch(['192.168.169.45:9205','192.168.169.46:9205','192.168.169.47:9205'], timeout=600)


query_body = {
        'query':{
                    'term':{'xnr_user_no':xnr_user_no}
                }
        }

xnr_flow_text_index_name_pre = 'xnr_flow_text_2018-11-29'
xnr_flow_text_index_type = 'text'

weibo_xnr_index_type = 'user'
es_results = es_xnr.search(index=xnr_flow_text_index_name_pre,doc_type=xnr_flow_text_index_type,body=query_body)

print json.dumps(es_results,ensure_ascii=False)

