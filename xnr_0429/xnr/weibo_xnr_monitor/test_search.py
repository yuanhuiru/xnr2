#_*_coding:utf-8_*_
import json
import sys
from elasticsearch import Elasticsearch
sys.path.append('/home/xnr1/xnr_0429/xnr')
sys.path.append('/home/xnr1/xnr_0429')
from parameter import MAX_DETECT_COUNT,MAX_FLOW_TEXT_DAYS,MAX_SEARCH_SIZE, FB_TW_TOPIC_ABS_PATH, FB_DOMAIN_ABS_PATH,\
                    DAY, WEEK, fb_tw_topic_en2ch_dict as topic_en2ch_dict, SENTIMENT_DICT_NEW, SORT_FIELD,\
                    TOP_KEYWORDS_NUM,TOP_WEIBOS_LIMIT,WEEK_TIME,DAY,DAY_HOURS,HOUR,\
                    fb_tw_topic_ch2en_dict as topic_ch2en_dict, WORD2VEC_PATH


es_xnr = Elasticsearch(['192.168.169.45:9205','192.168.169.47:9205','192.168.169.47:9206'], timeout=600)

#def search_by_keywords(keywords_list):
def search_by_keywords():
    keywords_list = ['民主', '维权', '血书', '加油', '反对']
    keyword_query_list = []
    query_item = 'text'
    count = MAX_DETECT_COUNT
    #for i in range(len(keywords_list)): 
    for keyword in keywords_list:
        #keyword_query_list.append({'wildcard':{query_item:'*'+keyword.encode('utf-8')+'*'}})
        keyword_query_list.append({'wildcard':{query_item:'*'+keyword+'*'}})
    query_body = {
        'query':{
            'bool':{
                'should':keyword_query_list,
            }
        },
        'size':50
    }
    print query_body
    query_body1 = {'query': {'match_all': {}}}
    es_result = es_xnr.search(index = 'weibo_sensitive_post_2019-01-04',doc_type='text',body=query_body)['hits']['hits']
    print json.dumps(es_result,ensure_ascii=False)
    print len(es_result)

def test():
    
    query_body1 = {'query': {'match_all': {}}}
    es_result = es_xnr.search(index = 'weibo_sensitive_post_2019-01-04',doc_type='text',body=query_body1)['hits']['hits']
    print es_result
if __name__ == '__main__':
    #search_by_keywords()
    #test()
    search_by_keywords()

