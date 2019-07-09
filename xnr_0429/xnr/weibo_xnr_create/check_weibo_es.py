#_*_coding:utf-8_*_
import json
from elasticsearch import Elasticsearch


es_xnr = Elasticsearch(['192.168.169.45:9205','192.168.169.46:9205','192.168.169.47:9205'], timeout=600)

weibo_xnr_index_name = 'weibo_xnr'
weibo_xnr_index_type = 'user'

def search_xnr_by_nickname():

    nick_name = 'xrodman'
    query_body = {
            'query':{
                    'term':{'nick_name':nick_name}
                }
            }

    es_results = es_xnr.get(index=weibo_xnr_index_name,doc_type=weibo_xnr_index_type,id='WXNR0152')
    monitor_keywords = es_results['_source']['monitor_keywords']




def search_all_xnr():
    query_body = {
        "query":{
            "match_all": {}
        },
        "size": 999
    }
    es_results = es_xnr.search(index=weibo_xnr_index_name,doc_type=weibo_xnr_index_type,body=query_body)['hits']['hits']
    for results in es_results:
        print results['_id']


def search_xnr_by_user_no():
    xnr_user_no = 'WXNR0006'
    es_results = es_xnr.get(index=weibo_xnr_index_name,doc_type=weibo_xnr_index_type,id='WXNR0006')
    print es_results

if __name__ == "__main__":
    #search_all_xnr()
    search_xnr_by_user_no()
