# -*- coding: utf-8 -*-
from elasticsearch import Elasticsearch
import arrow
import sys
sys.path.append('/home/xnr1/xnr_0429/xnr/sina')
from weibo_publish import weibo_publish_main


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


def publish(xnr_info):
    query_body = {
      'query': {
        'filtered': {
          'filter': {
            'bool': {
              'must': [
                {
                  'term': {
                    'compute_status': 0
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
    for result in results:
        r = result['_source']
        xnr_user_no = r.get('xnr_user_no')
        account = xnr_info.get(xnr_user_no, {}).get('account')
        password = xnr_info.get(xnr_user_no, {}).get('password')
        text = r['text']
        id = result['_id']
        if xnr_user_no and account and password:
            flag = weibo_publish_main(account, password, text)
            if flag:
                r['compute_status'] = 1
                print es_xnr.update(index=weibo_xnr_retweet_timing_list_index_name,
                         doc_type=weibo_xnr_retweet_timing_list_index_type,
                         id=id, body={'doc': r})



def main():
    xnr_info = load_xnr_info()
    publish(xnr_info)


if __name__ == '__main__':
    main()


