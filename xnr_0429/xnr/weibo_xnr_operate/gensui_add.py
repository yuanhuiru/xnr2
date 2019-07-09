# -*- coding: utf-8 -*-
from elasticsearch import Elasticsearch
import arrow


es_flow_text = Elasticsearch(['10.128.55.75:9200','10.128.55.76:9200'], timeout=600)
flow_text_index_name_pre = 'flow_text_' 	#flow_text_index_name: flow_text_2017-06-24
flow_text_index_type = 'text'


es_xnr = Elasticsearch(['192.168.169.45:9205','192.168.169.47:9205','192.168.169.47:9206'], timeout=600)
weibo_xnr_retweet_timing_list_index_name = 'tweet_retweet_timing_list'
weibo_xnr_retweet_timing_list_index_type = 'timing_list'


def load_xnr_info():
    res = {}
    search_res = es_xnr.search('weibo_xnr', 'user', {'size': 999})['hits']['hits']
    for item in search_res:
        source = item['_source']
        tw_mail_account = source.get('weibo_mail_account', '')
        tw_phone_account = source.get('weibo_phone_account', '')
        account = ''
        if tw_mail_account:
            account = tw_mail_account
        elif tw_phone_account:
            account = tw_phone_account
        if account:
            xnr_user_no = source.get('xnr_user_no', '')
            if xnr_user_no:
                res[xnr_user_no] = {}
                res[xnr_user_no]['account'] = account
                res[xnr_user_no]['password'] = source.get('password', '')
    return res


def search_gensui_userinfo(xnr_no):
    userinfo = {}
    query_body = {
      'query': {
        'filtered': {
          'filter': {
            'bool': {
              'must': [
                {
                  'term': {
                    'gensuiguanzhu': 1
                  }
                },
                {
                  'term': {
                    'xnr_no': xnr_no
                  }
                }
              ]
            }
          }
        }
      },
      'size': 999,
    }
    results = es_xnr.search('weibo_xnr_relations', 'user', query_body)['hits']['hits']
    for result in results:
        r = result['_source']
        userinfo[r['uid']] = r.get('nickname', r['uid'])
    return userinfo


def check_publish_status(uid, mid):
    query_body = {
      'query': {
        'filtered': {
          'filter': {
            'bool': {
              'must': [
                {
                  'term': {
                    'uid': uid
                  }
                },
                {
                  'term': {
                    'mid': mid
                  }
                }
              ]
            }
          }
        }
      },
      'size': 999,
    }
    results = es_xnr.search(weibo_xnr_retweet_timing_list_index_name, weibo_xnr_retweet_timing_list_index_type, query_body)['hits']['hits']
    if results:
        return True
    return False


def gensui(xnr_no, userinfo, date=arrow.now().format('YYYY-MM-DD')):
        query_body = {
            'query': {
                'filtered': {
                    'filter': {
                        'bool': {
                            'must': [
                                {
                                    'terms': {
                                        'uid': userinfo.keys()
                                    }
                                }
                            ]
                        }
                    }
                }
            },
            'size': 999,
        }
        results = es_flow_text.search(flow_text_index_name_pre+date, flow_text_index_type, query_body)['hits']['hits']
        for result in results:
            r = result['_source']
            uid = r['uid']
            mid = r['mid']
            d = {
                'xnr_user_no': xnr_no,
                'uid': uid,
                'nick_name': userinfo[r['uid']],
                'text': r['text'],
                'mid': mid,
                'timestamp': r['timestamp'],
                'timestamp_set': '', # 預計發佈時間
                'compute_status': 0,
                'photo_url': '',
            }
            if not check_publish_status(uid, mid):
                es_xnr.index(index=weibo_xnr_retweet_timing_list_index_name,
                         doc_type=weibo_xnr_retweet_timing_list_index_type,
                         body=d)


# only for test
def delete_gensui(date=arrow.now().format('YYYY-MM-DD')):
    query_body = {
        'query':{
            'match_all': {},
        },
        'size': 999
    }
    results = es_xnr.search(index=weibo_xnr_retweet_timing_list_index_name,
                 doc_type=weibo_xnr_retweet_timing_list_index_type,
                 body=query_body)['hits']['hits']
    for result in results:
        print es_xnr.delete(index=weibo_xnr_retweet_timing_list_index_name,
                 doc_type=weibo_xnr_retweet_timing_list_index_type,
                 id=result['_id'])


def main():
    xnr_info = load_xnr_info()
    for xnr_user_no in xnr_info:
        userinfo = search_gensui_userinfo(xnr_user_no)
        #delete_gensui()
        gensui(xnr_user_no, userinfo)


if __name__ == '__main__':
    main()


