# _*_coding:utf-8_*_
import json
import sys
import gensim
from elasticsearch import Elasticsearch
import time
sys.path.append('/home/xnr1/xnr_0429/xnr')
sys.path.append('/home/xnr1/xnr_0429')
from parameter import MAX_DETECT_COUNT, MAX_FLOW_TEXT_DAYS, MAX_SEARCH_SIZE, FB_TW_TOPIC_ABS_PATH, FB_DOMAIN_ABS_PATH, \
    DAY, WEEK, fb_tw_topic_en2ch_dict as topic_en2ch_dict, SENTIMENT_DICT_NEW, SORT_FIELD, \
    TOP_KEYWORDS_NUM, TOP_WEIBOS_LIMIT, WEEK_TIME, DAY, DAY_HOURS, HOUR, \
    fb_tw_topic_ch2en_dict as topic_ch2en_dict, WORD2VEC_PATH
from global_utils import es_flow_text,flow_text_index_name_pre, flow_text_index_type,es_xnr_2,\
    fb_xnr_index_name, fb_xnr_index_type,tw_xnr_index_name, tw_xnr_index_type,\
    facebook_flow_text_index_name_pre, facebook_flow_text_index_type,\
    twitter_flow_text_index_name_pre, twitter_flow_text_index_type

weibo_xnr_index_name = 'weibo_xnr'
weibo_xnr_index_type = 'user'
MAX_VALUE = 999


MAX_SEARCH_SIZE = 99


def ts2datetime(ts):
    return time.strftime('%Y-%m-%d', time.localtime(ts))


def datetime2ts(date):
    return int(time.mktime(time.strptime(date, '%Y-%m-%d')))


def load_index(index_prefix, from_ts, to_ts):
    index_list = []
    print index_prefix
    #print 'from_ts', [from_ts], 'to_ts', [to_ts]
    for timestamp in range(int(from_ts), int(to_ts) + 3600*24, 3600*24):
        index_list.append(index_prefix + ts2datetime(timestamp))
    return index_list


def load_fb_keywords(fb_xnr_user_no, extend_keywords_size=0):
    """
    :param xnr_user_no: 'WXNR0152' or 'ALL'(meaning all xnr)
    :param extend_keywords_size: extend keywords search size
    :return: monitor keywords
    """
    keywords = []

    if fb_xnr_user_no == 'ALL':
        query_body = {'query': {'term': {'create_status': 2}}, 'size': MAX_VALUE}
    else:
        query_body = {'query': {'term': {'xnr_user_no': fb_xnr_user_no}}}

    xnr_results = es_xnr_2.search(index=fb_xnr_index_name, doc_type=fb_xnr_index_type, body=query_body)['hits']['hits']
    for xnr_result in xnr_results:
        monitor_keywords = xnr_result['_source'].get('monitor_keywords', '')
        keywords.extend(monitor_keywords.split(','))

    return extend_keywords(list(set(keywords)), extend_keywords_size)



def load_tw_keywords(tw_xnr_user_no, extend_keywords_size=0):

    keywords = []

    if tw_xnr_user_no == 'ALL':
        query_body = {'query': {'term': {'create_status': 2}}, 'size': MAX_VALUE}
    else:
        query_body = {'query': {'term': {'xnr_user_no': tw_xnr_user_no}}}

    xnr_results = es_xnr_2.search(index=tw_xnr_index_name, doc_type=tw_xnr_index_type, body=query_body)['hits']['hits']
    for xnr_result in xnr_results:
        monitor_keywords = xnr_result['_source'].get('monitor_keywords', '')
        keywords.extend(monitor_keywords.split(','))
    return extend_keywords(list(set(keywords)), extend_keywords_size)



def extend_keywords(keywords, size):
    """
    :param keywords: [u'keyword1', u'keyword2', ...]
    :param size: extend keywords search size
    :return: extend keywords
    """
    extend_keywords = []
    if size:
        model = gensim.models.KeyedVectors.load_word2vec_format(WORD2VEC_PATH, binary=True)
        for keyword in keywords:
            try:
                # simi_list = [(u'\u5546\u4e1a', 0.788550853729248), (u'\u7ecf\u6d4e', 0.7600623369216919), ...]
                simi_list = model.most_similar(keyword, topn=size)
                for simi_word in simi_list:
                    extend_keywords.append(simi_word[0])
            except Exception, e:
                print u'扩展词 Exception:', str(e)
    extend_keywords.extend(keywords)
    return extend_keywords


def load_query_body(keywords, query_item='text'):
    query_body = {}
    if keywords:
        keyword_query_list = []
        for keyword in keywords:
            keyword_query_list.append({'wildcard': {query_item: '*' + keyword + '*'}})

        query_body = {
            'query': {
                'bool': {
                    'should': keyword_query_list,
                },
            },
            'size': MAX_SEARCH_SIZE
        }
    return  query_body


# 根据关键词查找监测帖子 facebook
def search_fb_posts(fb_xnr_user_no, from_ts, to_ts, extend_keywords_size=0):
    fb_keywords = load_fb_keywords(fb_xnr_user_no, extend_keywords_size)
    fb_query_body = load_query_body(fb_keywords)
    fb_index_list = load_index(facebook_flow_text_index_name_pre, from_ts, to_ts)
    print'222222222222 fb_index_list'
    print fb_index_list
    fb_search_results = es_xnr_2.search(index=fb_index_list, doc_type=facebook_flow_text_index_type, body=fb_query_body)['hits']['hits']
    return [item['_source'] for item in fb_search_results]


# 根据关键词查找监测帖子 twitter
def search_tw_posts(tw_xnr_user_no, from_ts, to_ts, extend_keywords_size=0):
    end_results = []
    tw_keywords = load_tw_keywords(tw_xnr_user_no, extend_keywords_size)
    tw_query_body = load_query_body(tw_keywords)
    tw_index_list = load_index(twitter_flow_text_index_name_pre, from_ts, to_ts)
    print '3333333333333333333 tw_index_list'
    print tw_index_list
    tw_search_results = es_xnr_2.search(index=tw_index_list, doc_type=twitter_flow_text_index_type, body=tw_query_body)['hits']['hits']
    return [item['_source'] for item in tw_search_results]



if __name__ == '__main__':
    # print search_fb_posts('WXNR0152', 1550139123, 1551003123)
    results = search_tw_posts('TXNR0024', 1566489600, 1566835200)
    print results




