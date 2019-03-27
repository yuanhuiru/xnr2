from elasticsearch import Elasticsearch 

query_body = {'query':{'match':{'task_status':0}}}


#fb_xnr_timing_list_index_name = 'fb_tweet_timing_list'
#fb_xnr_timing_list_index_type = 'timing_list'

ES_CLUSTER_HOST_2 = ['192.168.169.37:9206','192.168.169.38:9206']
es_xnr_2 = Elasticsearch(ES_CLUSTER_HOST_2, timeout=600)
#es_xnr_result = es_xnr_2.search(index=fb_xnr_timing_list_index_name,doc_type=fb_xnr_timing_list_index_type,body=query_body)['hits']['hits']
#es_xnr_result = es_xnr_2.delete(index=fb_xnr_timing_list_index_name,doc_type=fb_xnr_timing_list_index_type,id='null')
#es_xnr_result = es_xnr_2.delete_by_query(index=fb_xnr_timing_list_index_name,doc_type=fb_xnr_timing_list_index_type,body=query_body)

query = {'query':{'match_all':{}}}
fb_xnr_fans_followers_index_name ='fb_xnr_fans_followers'
fb_xnr_fans_followers_index_type='uids'

results = es_xnr_2.delete_by_query(index=fb_xnr_fans_followers_index_name,doc_type=fb_xnr_fans_followers_index_type,body=query)

