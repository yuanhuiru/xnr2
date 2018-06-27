# -*-coding:utf-8-*-
import re
import csv
import sys
import time
import json
import math
import redis
from elasticsearch import Elasticsearch
from datetime import datetime
from triple_sentiment_classifier import triple_classifier
from DFA_filter import createWordTree,searchWord 
from elasticsearch.helpers import scan
from global_utils_flow_text import black_words
reload(sys)
sys.path.append('../../')
from global_utils import R_CLUSTER_FLOW2 as r_cluster,twitter_flow_text_index_name_pre,\
                        twitter_flow_text_index_type,facebook_flow_text_index_name_pre,\
                        facebook_flow_text_index_type,\
                        facebook_user_index_name, facebook_user_index_type,\
                        twitter_user_index_name, twitter_user_index_type
from global_utils import es_xnr_2 as es, R_UNAME2ID_FT, fb_uname2id, tw_uname2id
from global_utils import R_ADMIN as r_sensitive
from parameter import sensitive_score_dict
from time_utils import ts2datetime,datetime2ts
from global_config import S_DATE_FB 


def ts2datetime_full(ts):
    return time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(ts))

def uname2uid(uname, ft_type):
    uid = R_UNAME2ID_FT.hget(ft_type,uname)
    if not uid:
        return 0
    return uid

def load_date_list(init_flag=False):
    date_list = []
    now_ts = int(time.time())
    if init_flag:   #如果是第一次运行，就加载系统启动至今的数据
        start_ts = datetime2ts(S_DATE_FB)
    else:   #只加载最近10天的数据
        start_ts = datetime2ts(ts2datetime(now_ts-10*24*3600))

    ts = start_ts
    while ts < now_ts:
        date = ts2datetime(ts)
        date_list.append(date)
        ts += 24*3600
    return date_list

#加载判定为中文用户的uid，对应用户的text内容不必翻译
def load_chinese_user(user_index, user_type, query_body={}):
    if not query_body:
        query_body = {
            'query': {
                'term': {'chinese_user': 1},
            },
            'size': 99999,
        }
    try:
        results = es.search(index=user_index, doc_type=user_type, body=query_body)['hits']['hits']
        uid_list = [result['_source']['uid'] for result in results]
    except Exception,e:
        print 'load_chinese_user Exception: ', str(e)
        uid_list = []
    return  uid_list

def count_flow_text_num(uid, flow_text_index_list, index_type, search_flag_ch=False):
    if search_flag_ch:
        textnum_query_body = {
            'query':{
                "filtered":{
                    "filter": {
                        "bool": {
                            "must": [
                                {"term": {"uid": uid}},
                                {"term": {"flag_ch": 1}},
                            ]
                         }
                    }
                }
            },
        }
    else:
        textnum_query_body = {
            'query':{
                "filtered":{
                    "filter": {
                        "bool": {
                            "must": [
                                {"term": {"uid": uid}},
                            ]
                         }
                    }
                }
            },
        }

    text_num = 0
    for index_name in flow_text_index_list:
        result = es.count(index=index_name, doc_type=index_type,body=textnum_query_body)
        if result['_shards']['successful'] != 0:
            text_num += result['count']
    return text_num

def update_chinese_user(index_name_pre, index_type, user_index_name, user_index_type, date_list):
    #user表中的chinese_user字段为1，代表是中文用户
    chinese_user_list= load_chinese_user(user_index_name, user_index_type)
    all_user_list = load_chinese_user(user_index_name, user_index_type, {'query':{'match_all':{},},'size':99999,})
    unchinese_user_list = list(set(all_user_list) - set(chinese_user_list))

    #需要进行update操作（更新chinese_user为1或0）的user_list
    update_list = []

    #流数据表
    flow_text_index_list = []
    for date in date_list:
        flow_text_index_list.append(index_name_pre + date)

    #对于chinese_user不为1的用户
    #当某个uid对应的所有flow_text中，中文帖子的比例占到一定数量，并且帖子数量大于一个阈值的时候，更新为chinese_user=1
    for uid in unchinese_user_list:
        text_num = count_flow_text_num(uid, flow_text_index_list, index_type)  
        if text_num >= 20:
            text_num_ch = count_flow_text_num(uid, flow_text_index_list, index_type, True)
            if float(text_num_ch)/text_num > 0.9:
                update_list.append((uid, 1))

    #对于chinese_user为1的用户，有选择的进行操作：更新为chinese_user=0
    for uid in chinese_user_list:
        text_num = count_flow_text_num(uid, flow_text_index_list, index_type)   
        if text_num:
            text_num += 1   #以防分母为0
            text_num_ch = count_flow_text_num(uid, flow_text_index_list, index_type, True)
            if float(text_num_ch)/text_num < 0.9:   #可以不发帖，但是如果发帖，中文帖子比例一定要高
                update_list.append((uid, 0))

    #统一进行update操作
    if update_list:
        bulk_update_action = []
        count = 0
        for uid,flag in update_list:
            update_action = {'update':{'_id': uid}}
            bulk_update_action.extend([update_action, {'doc': {'chinese_user': flag}}])
            count += 1
            if count % 1000 == 0:
                es.bulk(bulk_update_action, index=user_index_name, doc_type=user_index_type)
                bulk_update_action = []
        if bulk_update_action:
            es.bulk(bulk_update_action, index=user_index_name, doc_type=user_index_type)
    return True


def get_fb_root_retweet(text, root_uid):
    if isinstance(text, str):
        text = text.decode('utf-8', 'ignore')

    if text.startswith('Retweeted '):
        RE2 = re.compile('.*@(.*)\).*', re.UNICODE)
        repost_chains2 = RE2.findall(text)
        
        if repost_chains2 != []:
            root_uname = repost_chains2[0]
            root_uid = uname2uid(root_uname, fb_uname2id)
    elif ' shared ' in text:
        res = text.split(' shared ')
        root_con = res[1]
        if "'s" in root_con:
            root_uname = root_con.split("'s")[0]
            root_uid = uname2uid(root_uname, fb_uname2id)
        else:
            root_uname = ''
            root_uid = 0        
    else:
        root_uname = ''
        root_uid = 0
    return root_uid,root_uname

def get_tw_root_retweet(text, root_uid):
    if isinstance(text, str):
        text = text.decode('utf-8', 'ignore')
    if text.startswith('RT '):
        try:
            root_uname = text.split('RT @')[1].split(':')[0]
            root_uid = uname2uid(root_uname,tw_uname2id)
            if not directed_uid:
                root_uid = 0
                root_uname = root_uname
        except:
            root_uname = 0
            root_uid = ''
    else:
        root_uname = 0
        root_uid = ''
    return root_uid, root_uname

#for retweet message: get directed retweet uname and uid
#input: text, root_uid
#output: directed retweet uid and uname
def get_root_retweet(text, root_uid, ft_type):
    if ft_type == 'facebook':
        return get_fb_root_retweet(text, root_uid)
    else:
        return get_tw_root_retweet(text, root_uid)


#get weibo keywords_dict and keywords_string
#write in version: 15-12-08
#input: keyowrds_list
#output: keywords_dict, keywords_string
def get_weibo_keywords(keywords_list):
    keywords_dict = {}
    keywords_string = ''
    filter_keywords_set = set()
    for word in keywords_list:
        if word not in black_words:
            try:
                keywords_dict[word] += 1
            except:
                keywords_dict[word] = 1
            filter_keywords_set.add(word)
    keywords_string = '&'.join(list(filter_keywords_set))
    return keywords_dict, keywords_string

def test(ft_type):
    print ft_type
    if ft_type == 'facebook':
        index_name_pre = facebook_flow_text_index_name_pre
        index_type = facebook_flow_text_index_type
        user_index_name = facebook_user_index_name
        user_index_type = facebook_user_index_type
    else:
        index_name_pre = twitter_flow_text_index_name_pre
        index_type = twitter_flow_text_index_type
        user_index_name = twitter_user_index_name
        user_index_type = twitter_user_index_type

    # date_list = load_date_list(True)
    date_list = load_date_list()


    DFA = createWordTree()
    query_body = {
      'post_filter': {
        'missing': {
          'field': 'keywords_string'
        }
      },
      'query': {
        'filtered': {
          'filter': {
            'bool': {
              'must': [
                {
                  'range': {
                    'flag_ch': {
                      'gte': -1
                    }
                  }
                }
              ]
            }
          }
        }
      }
    }
    for date in date_list:
        count = 0
        bulk_action = []
        index_name = index_name_pre + date
        try:
            es_scan_results = scan(es,query=query_body,size=1000,index=index_name,doc_type=index_type)
            while True:
                try:
                    scan_data = es_scan_results.next()
                    item = scan_data['_source']
                    text = item['text_ch']
                    uid = item['uid']
                    if ft_type == 'facebook':
                        _id = item['fid']
                    else:
                        _id = item['tid']

                    ts = datetime2ts(date)
                    #add sentiment field to weibo
                    
                    sentiment, keywords_list  = triple_classifier(item)

                    #add key words to weibo
                    keywords_dict, keywords_string = get_weibo_keywords(keywords_list)

                    #sensitive_words_dict
                    sensitive_words_dict = searchWord(text.encode('utf-8', 'ignore'), DFA)
                    if sensitive_words_dict:
                        sensitive_words_string_data = "&".join(sensitive_words_dict.keys())
                        sensitive_words_dict_data = json.dumps(sensitive_words_dict)
                    else:
                        sensitive_words_string_data = ""
                        sensitive_words_dict_data = json.dumps({})

                    #redis
                    if sensitive_words_dict:
                        sensitive_count_string = r_cluster.hget('sensitive_'+str(ts), str(uid))
                        if sensitive_count_string: #redis取空
                            sensitive_count_dict = json.loads(sensitive_count_string)
                            for word in sensitive_words_dict.keys():
                                if sensitive_count_dict.has_key(word):
                                    sensitive_count_dict[word] += sensitive_words_dict[word]
                                else:
                                    sensitive_count_dict[word] = sensitive_words_dict[word]
                            r_cluster.hset('sensitive_'+str(ts), str(uid), json.dumps(sensitive_count_dict))
                        else:
                            r_cluster.hset('sensitive_'+str(ts), str(uid), json.dumps(sensitive_words_dict))

                    #sensitive
                    sensitive_score = 0
                    if sensitive_words_dict:
                        for k,v in sensitive_words_dict.iteritems():
                            tmp_stage = r_sensitive.hget("sensitive_words", k)
                            if tmp_stage:
                                sensitive_score += v*sensitive_score_dict[str(tmp_stage)]

                    #directed_uid
                    directed_uid_data = 0
                    directed_uid, directed_uname = get_root_retweet(text, uid, ft_type)
                    if directed_uid:
                        directed_uid_data = long(directed_uid)

                    # hashtag
                    hashtag = ''
                    RE = re.compile(u'#([0-9a-zA-Z-_⺀-⺙⺛-⻳⼀-⿕々〇〡-〩〸-〺〻㐀-䶵一-鿃豈-鶴侮-頻並-龎]+)[ ," =.。： :、]')
                    hashtag_list = re.findall(RE,text)
                    if hashtag_list:
                        hashtag = '&'.join(hashtag_list)

                    #action
                    action = {'update': {'_id': _id}}

                    # action_data
                    action_data = {
                        'sentiment': str(sentiment),
                        'keywords_dict': json.dumps(keywords_dict), 
                        'keywords_string': keywords_string,
                        'sensitive_words_string': sensitive_words_string_data,
                        'sensitive_words_dict': sensitive_words_dict_data,
                        'sensitive': sensitive_score,
                        'directed_uid': directed_uid_data,
                        'directed_uname': directed_uname,
                        'hashtag': hashtag,
                    }

                    bulk_action.extend([action, {'doc': action_data}])
                    count += 1
                    
                    if count % 1000 == 0 and count != 0:
                        if bulk_action:
                            es.bulk(bulk_action, index=index_name, doc_type=facebook_flow_text_index_type, timeout=600)
                        bulk_action = []
                        count = 0
                except StopIteration:
                    break
            if bulk_action:

                es.bulk(bulk_action, index=index_name, doc_type=facebook_flow_text_index_type, timeout=600)
        except Exception,e: #es文档不存在
            print e
            
    #这个更新比较费时，设定只允许每天5点以前的更新请求
    if int(ts2datetime_full(time.time()).split(' ')[1][:2]) < 5:
        print 'update chinese user'
        update_chinese_user(index_name_pre, index_type, user_index_name, user_index_type, date_list)

if __name__ == '__main__':
    print 'start time: ', time.time()
    test('facebook')
    test('twitter')

