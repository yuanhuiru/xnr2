# -*-coding:utf-8-*-

# -*- coding: utf-8 -*-
import json
import elasticsearch
from elasticsearch import Elasticsearch
import sys
sys.path.append('/home/xnr1/xnr_0429/xnr/')
from global_utils import es_xnr as es, weibo_xnr_index_name, weibo_xnr_index_type


def save_error_es(username, account_type):
    if account_type == 'phone':
        print '_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_错误的登录'
        body = {"query":{"term":{"weibo_phone_account":username}}}
        item_exist = es.search(index=weibo_xnr_index_name,doc_type=weibo_xnr_index_type, body=body)['hits']['hits']
        xnr_id = item_exist[0]['_id']
        print es.update(index=weibo_xnr_index_name,doc_type=weibo_xnr_index_type,id=xnr_id,body={'doc':{'verify_password':username}})

    elif account_type == "mail":
        print '_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_错误的登录'
        body = {"query":{"term":{"weibo_mail_account":username}}}
        item_exist = es.search(index=weibo_xnr_index_name,doc_type=weibo_xnr_index_type, body=body)['hits']['hits']
        xnr_id = item_exist[0]['_id']
        print es.update(index=weibo_xnr_index_name,doc_type=weibo_xnr_index_type,id=xnr_id,body={'doc':{'verify_password':username}})
        print 'mail'
    else:
        print '_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_正确的登录'
        body = {"query":{"term":{"weibo_mail_account":username}}}
        body2 = {"query":{"term":{"weibo_phone_account":username}}}
        item_exist = es.search(index=weibo_xnr_index_name,doc_type=weibo_xnr_index_type, body=body)['hits']['hits']
        if item_exist:
            xnr_id = item_exist[0]['_id']
            print es.update(index=weibo_xnr_index_name,doc_type=weibo_xnr_index_type,id=xnr_id,body={'doc':{'verify_password':''}})
        else:
            item_exist = es.search(index=weibo_xnr_index_name,doc_type=weibo_xnr_index_type, body=body2)['hits']['hits']
            xnr_id = item_exist[0]['_id']
            print es.update(index=weibo_xnr_index_name,doc_type=weibo_xnr_index_type,id=xnr_id,body={'doc':{'verify_password':''}})


if __name__ == '__main__':
    #username = '80617252@qq.com'
    #account_type = 'mail'
    #save_error_es(username, account_type)
    pass
