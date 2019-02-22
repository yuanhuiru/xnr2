# _*_coding:utf-8_*_
import json
import sys
import gensim
from elasticsearch import Elasticsearch

sys.path.append('/home/xnr1/xnr_0429/xnr')
sys.path.append('/home/xnr1/xnr_0429')
from parameter import MAX_DETECT_COUNT, MAX_FLOW_TEXT_DAYS, MAX_SEARCH_SIZE, FB_TW_TOPIC_ABS_PATH, FB_DOMAIN_ABS_PATH, \
    DAY, WEEK, fb_tw_topic_en2ch_dict as topic_en2ch_dict, SENTIMENT_DICT_NEW, SORT_FIELD, \
    TOP_KEYWORDS_NUM, TOP_WEIBOS_LIMIT, WEEK_TIME, DAY, DAY_HOURS, HOUR, \
    fb_tw_topic_ch2en_dict as topic_ch2en_dict, WORD2VEC_PATH
from global_utils import es_flow_text,flow_text_index_name_pre, flow_text_index_type

weibo_xnr_index_name = 'weibo_xnr'
weibo_xnr_index_type = 'user'
es_xnr = Elasticsearch(['192.168.169.45:9205', '192.168.169.47:9205', '192.168.169.47:9206'], timeout=600)
MAX_VALUE = 999



def load_keywords(xnr_user_no, extend_keywords_size=0):
    """
    :param xnr_user_no: 'WXNR0152' or 'ALL'(meaning all xnr)
    :param extend_keywords_size: extend keywords search size
    :return: monitor keywords
    """
    keywords = []

    if xnr_user_no == 'ALL':
        query_body = {'query': {'term': {'create_status': 2}}, 'size': MAX_VALUE}
    else:
        query_body = {'query': {'term': {'xnr_user_no': xnr_user_no}}}

    xnr_results = es_xnr.search(index=weibo_xnr_index_name, doc_type=weibo_xnr_index_type, body=query_body)['hits']['hits']
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
                    'must': keyword_query_list,
                }
            },
        }

    return  query_body

def search_posts():
    keywords = load_keywords('WXNR0152', 2)
    query_body = load_query_body(keywords)

    date = '2019-01-20'
    flow_text_index_name = '%s%s' % (flow_text_index_name_pre, date)

    print 'keywords', keywords
    print 'query_body', query_body
    search_results = es_flow_text.search(index=flow_text_index_name, doc_type=flow_text_index_type, body=query_body)['hits']['hits']
    for r in search_results:
        print r['_score'], r['_souce']['text']
    return 'search_results', search_results



if __name__ == '__main__':
    #detect_by_keywords()
    # print load_keywords('WXNR0152', 2)
    search_posts()









