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
                        facebook_flow_text_index_type
from global_utils import es_xnr_2 as es, R_UNAME2ID_FT, fb_uname2id, tw_uname2id
from global_utils import R_ADMIN as r_sensitive
from parameter import sensitive_score_dict
from time_utils import ts2datetime,datetime2ts
from global_config import S_DATE_FB 



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
    else:   #只加载最近7天的数据
        start_ts = datetime2ts(ts2datetime(now_ts-7*24*3600))

    ts = start_ts
    while ts < now_ts:
        date = ts2datetime(ts)
        date_list.append(date)
        ts += 24*3600
    return date_list

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
    else:
        index_name_pre = twitter_flow_text_index_name_pre
        index_type = twitter_flow_text_index_type


    DFA = createWordTree()
    date_list = load_date_list(True)
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

if __name__ == '__main__':
    test('facebook')
    test('twitter')

