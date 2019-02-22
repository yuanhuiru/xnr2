#_*_coding:utf-8_*_
import json
import gensim
import sys
from elasticsearch import Elasticsearch
sys.path.append('/home/xnr1/xnr_0429/xnr')
sys.path.append('/home/xnr1/xnr_0429')
from parameter import MAX_DETECT_COUNT,MAX_FLOW_TEXT_DAYS,MAX_SEARCH_SIZE, FB_TW_TOPIC_ABS_PATH, FB_DOMAIN_ABS_PATH,\
                    DAY, WEEK, fb_tw_topic_en2ch_dict as topic_en2ch_dict, SENTIMENT_DICT_NEW, SORT_FIELD,\
                    TOP_KEYWORDS_NUM,TOP_WEIBOS_LIMIT,WEEK_TIME,DAY,DAY_HOURS,HOUR,\
                    fb_tw_topic_ch2en_dict as topic_ch2en_dict, WORD2VEC_PATH


weibo_xnr_index_name = 'weibo_xnr'
weibo_xnr_index_type = 'user'
es_xnr = Elasticsearch(['192.168.169.45:9205','192.168.169.47:9205','192.168.169.47:9206'], timeout=600)


def detect_by_keywords():

    keywords_list = []
    es_xnr = Elasticsearch(['192.168.169.45:9205','192.168.169.47:9205','192.168.169.47:9205'], timeout=600)

    es_results = es_xnr.get(index=weibo_xnr_index_name,doc_type=weibo_xnr_index_type,id='WXNR0152')
    monitor_keywords = es_results['_source']['monitor_keywords']
    #print es_results
    #print list(monitor_keywords)
    monitor_keywords = u"共产党,中国"
    print monitor_keywords
    model = gensim.models.KeyedVectors.load_word2vec_format(WORD2VEC_PATH,binary=True)
    for word in monitor_keywords.split(','):
        print '-==-========-=-=-=-=-=-=-=-=-=-'
        print word
        if word == ',':
            continue
        keywords_list.append(word)
        try:
            simi_list = model.most_similar(word,topn=20)
            for simi_word in simi_list:
                keywords_list.append(simi_word[0])
        except Exception, e:
            print u'扩展词 Exception', str(e)
    print '开始搜索 start searching================================'
    print keywords_list
    #search_by_keywords(keywords_list)


def search_by_keywords(keywords_list):
    
    keyword_query_list = []
    query_item = 'text'
    count = MAX_DETECT_COUNT
    for i in range(len(keywords_list)): 
        keyword = keywords_list[i]
        keyword_query_list.append({'wildcard':{query_item:'*'+keyword+'*'}})
    query_body = {
        'query':{
            'bool':{
                'should':keyword_query_list,
            }
        },
    }
    es_result = es_xnr.search(index = 'weibo_sensitive_post_2019-01-04',doc_type='text',body=query_body)['hits']['hits']
    print json.dumps(es_result, ensure_ascii=False)
    print len(es_result)
if __name__ == '__main__':
    detect_by_keywords()
