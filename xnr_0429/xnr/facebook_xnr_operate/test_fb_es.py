from elasticsearch import Elasticsearch

fb_xnr_index_name = 'fb_xnr'
fb_xnr_index_type = 'user'
ES_CLUSTER_HOST_2 = ['192.168.169.37:9206','192.168.169.38:9206']

es_xnr_2 = Elasticsearch(ES_CLUSTER_HOST_2, timeout=600)

es_xnr_result = es_xnr_2.get(index=fb_xnr_index_name,doc_type=fb_xnr_index_type,id=xnr_user_no)['_source']
print es_xnr_result
