# -*-coding:utf-8-*-

# -*- coding: utf-8 -*-
import json
import elasticsearch
from elasticsearch import Elasticsearch
import sys
sys.path.append('../../')
from global_utils import es_xnr as es, weibo_xnr_index_name, weibo_xnr_index_type


def save_error_es(username, account_type):
    if account_type == 'phone':
        body = {"query":{"term":{"weibo_phone_account":username}}}
        print 'phone'
    else:
        body = {"query":{"term":{"weibo_mail_account":username}}}
        print 'mail'
    item_exist = es.search(index=weibo_xnr_index_name,doc_type=weibo_xnr_index_type, body=body)['hits']['hits']
    print item_exist
    print item_exist[0]['_id']
    xnr_id = item_exist[0]['_id']
    print es.update(index=weibo_xnr_index_name,doc_type=weibo_xnr_index_type,id=xnr_id,body={'doc':{'verify_password':username}})



if __name__ == '__main__':
    username = '80617252@qq.com'
    account_type = 'mail'
    save_error_es(username, account_type)
