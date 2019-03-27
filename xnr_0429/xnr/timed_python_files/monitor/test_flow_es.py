import json
from elasticsearch import Elasticsearch


ES_FLOW_TEXT_HOST = ['10.128.55.75:9200','10.128.55.76:9200']
es_flow_text = Elasticsearch(ES_FLOW_TEXT_HOST, timeout=600)

flow_text_index_name_list = ['flow_text_2018-12-27']
flow_text_index_type = 'text'

query_body = {'query':{'match_all':{}},'size':100}


es_result = es_flow_text.search(index=flow_text_index_name_list, doc_type=flow_text_index_type, body = query_body)

print json.dumps(es_result, ensure_ascii=False)

