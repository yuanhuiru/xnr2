#-*- coding: utf-8 -*-
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
from pymongo import MongoClient
from bson.objectid import ObjectId
from langconv import *
import langid
import datetime
import re
import json
import time
import subprocess
import os
from facebook_mappings import facebook_flow_text_mappings,facebook_count_mappings,facebook_user_mappings
from twitter_mappings import twitter_flow_text_mappings,twitter_count_mappings,twitter_user_mappings
from global_utils import r, es_378 as es, es_4567, twitter_flow_text_index_name_pre,twitter_flow_text_index_type,\
                    twitter_count_index_name_pre,twitter_count_index_type,\
                    twitter_user_index_name,twitter_user_index_type,\
                    facebook_flow_text_index_name_pre,facebook_flow_text_index_type,\
                    facebook_count_index_name_pre,facebook_count_index_type,\
                    facebook_user_index_name,facebook_user_index_type,\
                    twitter_flow_text_trans_task_name,facebook_flow_text_trans_task_name,\
                    twitter_user_trans_task_name,facebook_user_trans_task_name,\
                    fb_uname2id, tw_uname2id, R_UNAME2ID_FT
from global_config import mongodb_host, mongodb_port, mysql_host, mysql_port, mysql_user, mysql_pwd,\
                    SYSTEM_START_TIME


def ts2datetime(ts):
    return time.strftime('%Y-%m-%d', time.localtime(ts))

def datetime2ts(date):
    return int(time.mktime(time.strptime(date, '%Y-%m-%d')))

def date2ts(date):
    return int(time.mktime(time.strptime(date, '%Y-%m-%d %H:%M:%S')))

def ts2datetime_full(ts):
    return time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(ts))

#繁体转简体
def traditional2simplified(sentence):
    '''
    将sentence中的繁体字转为简体字
    :param sentence: 待转换的句子
    :return: 将句子中繁体字转换为简体字之后的句子
    '''
    sentence = Converter('zh-hans').convert(sentence)
    return sentence

#简体转繁体
def simplified2traditional(sentence):  
    sentence = Converter('zh-hant').convert(sentence)  
    return sentence  

emoji_pattern = re.compile(
    u"(\ud83d[\ude00-\ude4f])|"  # emoticons
    u"(\ud83c[\udf00-\uffff])|"  # symbols & pictographs (1 of 2)
    u"(\ud83d[\u0000-\uddff])|"  # symbols & pictographs (2 of 2)
    u"(\ud83d[\ude80-\udeff])|"  # transport & map symbols
    u"(\ud83c[\udde0-\uddff])|"  # flags (iOS)
    u"[\U00010000-\U0010ffff]"   # facebook emoji
    "+", flags=re.UNICODE)

def remove_emoji(text):
    return emoji_pattern.sub(r'', text)

#select * from user_2017 where 
#(updatetime >= '2018-03-31 12:00:00' and updatetime < '2018-05-27 13:00:07')
#mysql的时间范围限制，一般指updatetime，保证从上一次存储结束的时间开始存储
def load_sql_time_tail(timefiled, flush_flag=False, flush_dt=''):
    # dt1, dt2 = load_time_range()
    # ##仅保存这个时间点以后【创建】的帖子、用户等信息
    if flush_flag:  #delete 用
        sql_tail = " ( " + timefiled + " <= '" + flush_dt +  " ' ) "
    else:   # select 用
        if timefiled in ['createtime', 'createdat', 'createat']:    
            sql_tail = " ( " + timefiled + " >= '" + SYSTEM_START_TIME +  " ' ) "
        ##仅保存这个时间段内【更新】的帖子、用户等信息
        else:                   
            sql_tail = " ( " + timefiled + " >= '" + dt1 + "' and " + timefiled + " < '" + dt2 + "'" + " ) "
    return sql_tail

#mongo的时间限制query，一般指updated_time，保证从上一次存储结束的时间开始存储
def load_mongodb_time_query(timefiled, flush_flag=False, flush_dt=''):
    #这里timefiled是一个list
    # dt1, dt2 = load_time_range()
    query = {}
    if flush_flag:  #delete 用
        for tf in timefiled:
            query[tf] = {
                '$lte': 'T'.join(flush_dt.split(' '))
            }
    else:           # select 用
        for tf in timefiled:
            # ##仅保存这个时间点以后【创建】的帖子、用户等信息
            if  tf in ['created_time']:
                query[tf] = {
                    '$gte': 'T'.join(SYSTEM_START_TIME.split(' '))
              }
            ##仅保存这个时间段内【更新】的帖子、用户等信息
            else:
                query[tf] = {
                    '$gte': 'T'.join(dt1.split(' ')),
                    '$lt': 'T'.join(dt2.split(' '))
              }
    return query

#因为北京时间和数据库的时间有时差，所以在查询的时候要进行处理，在存数据库的时候也要处理。
#北京0点   数据库当天15点
def update_last_ts(dt):
    ts = date2ts(dt)
    # ts -= 15*3600
    with open('last_update_ts.txt', 'a') as f:
        f.write(str(ts) + '\n')

def load_last_update_ts():
    ts_list = []
    with open('last_update_ts.txt', 'r') as f:
        for line in f:
            ts_list.append(line.strip())
    return float(ts_list[-1])

#因为北京时间和数据库的时间有时差，所以在查询的时候要进行处理，在存数据库的时候也要处理。
#北京0点   数据库当天15点
def load_time_range():
    ts1 = load_last_update_ts()
    # ts1 += 15*3600
    dt1 = ts2datetime_full(ts1)

    ts2 = time.time()
    # ts2 += 15*3600
    dt2 = ts2datetime_full(ts2)
    return dt1, dt2

dt1, dt2 = load_time_range()

def create_mappings():
    date = dt2.split(' ')[0]
    ts = datetime2ts(date) + 24*3600
    date = ts2datetime(ts)
    facebook_user_mappings()
    facebook_count_mappings(facebook_count_index_name_pre + date)
    facebook_flow_text_mappings(facebook_flow_text_index_name_pre + date)
    twitter_user_mappings()
    twitter_count_mappings(twitter_count_index_name_pre + date)
    twitter_flow_text_mappings(twitter_flow_text_index_name_pre + date)

def connect_mongodb(host, port, dbname, cname, query={}, query_field={}):
    client = MongoClient(host, port)
    db = client[dbname]
    c = db[cname]
    if query_field:
        res = c.find(query, query_field) #返回值是pymongo.cursor.Cursor object
    else:
        res = c.find(query)
    return list(res)

def connect_mysqldb(sql, host, port, user, pwd, dbname, tablename):
    import MySQLdb
    conn = MySQLdb.connect(host=host,port=port,user=user,passwd=pwd,db=dbname,charset='utf8')
    cursor = conn.cursor()
    cursor.execute(sql)
    res =  cursor.fetchall()
    conn.close()
    return res

def flush_mongodb(host, port, dbname, cname, query):
    client = MongoClient(host, port)
    db = client[dbname]
    c = db[cname]
    try:
        c.remove(query)
        return True
    except Exception,e:
        print 'flush_mongodb Exception: ', str(e)
        return False

def flush_mysqldb(sql, host, port, user, pwd, dbname, tablename):
    import MySQLdb
    conn = MySQLdb.connect(host=host,port=port,user=user,passwd=pwd,db=dbname,charset='utf8')
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        conn.commit()
        conn.close()
        return True
    except Exception,e:
        print 'flush_mysqldb Exception: ', str(e)
        conn.rollback()
        conn.close()
        return False

def check_mysqldb():
    flag = False
    import MySQLdb
    version = MySQLdb.__version__
    if version == '1.2.3':
        flag = True
    return flag

    # try:
    #     import MySQLdb
    #     version = MySQLdb.__version__
    #     if version == '1.2.3':
    #         flag = True
    # except Exception,e:
    #     print 'check MySQLdb Exception: ', str(e)

    # if not flag:
    #     install_cmd = 'sudo apt-get install python-mysqldb=1.2.3-2ubuntu1'
    #     print 'install_cmd: ', install_cmd
    #     os.popen(install_cmd)
    #     '''
    #     p = subprocess.Popen(install_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)  
    #     for line in p.stdout.readlines():  
    #         print line
    #     '''
    # import MySQLdb
    # version = MySQLdb.__version__
    # print version

# 清空now之前一段时间（默认30天）的清华数据库的数据
# 按照updatetime
def flush_data(period=30):
    # now: 
    now_ts = date2ts(dt2)
    ts = now_ts - period*24*3600
    flush_dt = ts2datetime_full(ts)
    print 'flush_dt: ', flush_dt

    result = False

    #T2MS_bh.user_2017
    sql_T2MS_bh_user_2017 = 'delete from user_2017 ' + ' where ' + load_sql_time_tail(timefiled='updatetime', flush_flag=True, flush_dt=flush_dt)
    result = flush_mysqldb(sql_T2MS_bh_user_2017, mysql_host, mysql_port, mysql_user, mysql_pwd, 'T2MS_bh', 'user_2017')
    print 'remove T2MS_bh.user_2017: ', sql_T2MS_bh_user_2017, result
    
    #T2MS_bh.tweet_2017
    sql_T2MS_bh_tweet_2017 = 'delete from tweet_2017 ' + ' where ' + load_sql_time_tail(timefiled='updatetime', flush_flag=True, flush_dt=flush_dt)
    result = flush_mysqldb(sql_T2MS_bh_tweet_2017, mysql_host, mysql_port, mysql_user, mysql_pwd, 'T2MS_bh', 'tweet_2017')
    print 'remove T2MS_bh.tweet_2017: ', sql_T2MS_bh_tweet_2017, result

    #T2MS_bh.relation_2017
    sql_T2MS_bh_relation_2017 = 'delete from relation_2017 ' + ' where ' + load_sql_time_tail(timefiled='updatetime', flush_flag=True, flush_dt=flush_dt)
    result = flush_mysqldb(sql_T2MS_bh_relation_2017, mysql_host, mysql_port, mysql_user, mysql_pwd, 'T2MS_bh', 'relation_2017')
    print 'remove T2MS_bh.relation_2017: ', sql_T2MS_bh_relation_2017, result

    #T2MS_bh.id_2017
    sql_T2MS_bh_id_2017 = 'delete from id_2017 ' + ' where ' + load_sql_time_tail(timefiled='updatetime', flush_flag=True, flush_dt=flush_dt)
    result = flush_mysqldb(sql_T2MS_bh_id_2017, mysql_host, mysql_port, mysql_user, mysql_pwd, 'T2MS_bh', 'id_2017')
    print 'remove T2MS_bh.id_2017: ', sql_T2MS_bh_id_2017, result

    #streaming_bh.Tuser
    sql_streaming_bh_Tuser = 'delete from Tuser ' + ' where ' + load_sql_time_tail(timefiled='updatetime', flush_flag=True, flush_dt=flush_dt)
    result = flush_mysqldb(sql_streaming_bh_Tuser, mysql_host, mysql_port, mysql_user, mysql_pwd, 'streaming_bh', 'Tuser')
    print 'remove streaming_bh.Tuser: ', sql_streaming_bh_Tuser, result

    #streaming_bh.tweet
    sql_streaming_bh_tweet = 'delete from tweet ' + ' where ' + load_sql_time_tail(timefiled='updatetime', flush_flag=True, flush_dt=flush_dt)
    result = flush_mysqldb(sql_streaming_bh_tweet, mysql_host, mysql_port, mysql_user, mysql_pwd, 'streaming_bh', 'tweet')
    print 'remove streaming_bh.tweet: ', sql_streaming_bh_tweet, result

    #streaming_bh.TIDscouting
    sql_streaming_bh_TIDscouting = 'delete from TIDscouting ' + ' where ' + load_sql_time_tail(timefiled='updatetime', flush_flag=True, flush_dt=flush_dt)
    result = flush_mysqldb(sql_streaming_bh_TIDscouting, mysql_host, mysql_port, mysql_user, mysql_pwd, 'streaming_bh', 'TIDscouting')
    print 'remove streaming_bh.TIDscouting: ', sql_streaming_bh_TIDscouting, result

    #streaming_bh.Tscouting
    sql_streaming_bh_Tscouting = 'delete from Tscouting ' + ' where ' + load_sql_time_tail(timefiled='updatetime', flush_flag=True, flush_dt=flush_dt)
    result = flush_mysqldb(sql_streaming_bh_Tscouting, mysql_host, mysql_port, mysql_user, mysql_pwd, 'streaming_bh', 'Tscouting')
    print 'remove streaming_bh.Tscouting: ', sql_streaming_bh_Tscouting, result

    #streaming_bh.Fuser
    sql_streaming_bh_Fuser = 'delete from Fuser ' + ' where ' + load_sql_time_tail(timefiled='updatetime', flush_flag=True, flush_dt=flush_dt)
    result = flush_mysqldb(sql_streaming_bh_Fuser, mysql_host, mysql_port, mysql_user, mysql_pwd, 'streaming_bh', 'Fuser')
    print 'remove streaming_bh.Fuser: ', sql_streaming_bh_Fuser, result

    #streaming_bh.FIDscouting
    sql_streaming_bh_FIDscouting = 'delete from FIDscouting ' + ' where ' + load_sql_time_tail(timefiled='updatetime', flush_flag=True, flush_dt=flush_dt)
    result = flush_mysqldb(sql_streaming_bh_FIDscouting, mysql_host, mysql_port, mysql_user, mysql_pwd, 'streaming_bh', 'FIDscouting')
    print 'remove streaming_bh.FIDscouting: ', sql_streaming_bh_FIDscouting, result

    #streaming_bh.Fscouting
    sql_streaming_bh_Fscouting = 'delete from Fscouting ' + ' where ' + load_sql_time_tail(timefiled='updatetime', flush_flag=True, flush_dt=flush_dt)
    result = flush_mysqldb(sql_streaming_bh_Fscouting, mysql_host, mysql_port, mysql_user, mysql_pwd, 'streaming_bh', 'Fscouting')
    print 'remove streaming_bh.Fscouting: ', sql_streaming_bh_Fscouting, result

    #TsingHuaFBTest_bh.post
    query_TsingHuaFBTest_bh_post = load_mongodb_time_query(timefiled=['updated_time'], flush_flag=True, flush_dt=flush_dt)
    result = flush_mongodb(mongodb_host, mongodb_port, 'TsingHuaFBTest_bh', 'post', query=query_TsingHuaFBTest_bh_post)
    print 'remove TsingHuaFBTest_bh.post: ', query_TsingHuaFBTest_bh_post, result

    #TsingHuaFBTest_bh.user
    query_TsingHuaFBTest_bh_user = load_mongodb_time_query(timefiled=['updated_time'], flush_flag=True, flush_dt=flush_dt)
    result = flush_mongodb(mongodb_host, mongodb_port, 'TsingHuaFBTest_bh', 'user', query=query_TsingHuaFBTest_bh_user)
    print 'remove TsingHuaFBTest_bh.user: ', query_TsingHuaFBTest_bh_user, result

    #TsingHuaFBTest_bh.relation
    query_TsingHuaFBTest_bh_relation = load_mongodb_time_query(timefiled=[], flush_flag=True, flush_dt=flush_dt)
    result = flush_mongodb(mongodb_host, mongodb_port, 'TsingHuaFBTest_bh', 'relation', query=query_TsingHuaFBTest_bh_relation)
    print 'remove TsingHuaFBTest_bh.relation: ', query_TsingHuaFBTest_bh_relation, result

    #TsingHuaFBTest_bh.id
    query_TsingHuaFBTest_bh_id = load_mongodb_time_query(timefiled=[], flush_flag=True, flush_dt=flush_dt)
    result = flush_mongodb(mongodb_host, mongodb_port, 'TsingHuaFBTest_bh', 'id', query=query_TsingHuaFBTest_bh_id)
    print 'remove TsingHuaFBTest_bh.id: ', query_TsingHuaFBTest_bh_id, result

    #FBStreaming_bh.post
    query_FBStreaming_bh_post = load_mongodb_time_query(timefiled=['updated_time'], flush_flag=True, flush_dt=flush_dt)
    result = flush_mongodb(mongodb_host, mongodb_port, 'FBStreaming_bh', 'post', query=query_FBStreaming_bh_post)
    print 'remove FBStreaming_bh.post: ', query_FBStreaming_bh_post, result

def convert_fb_time(t):
    date,time = t.split('T')
    time = time.split('+')[0]
    datetime = date + ' ' + time
    ts = date2ts(datetime)
    return ts

#加载判定为中文用户的uid，对应用户的text内容不必翻译
def load_chinese_user(user_index, user_type):
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

def my_bulk_func(bulk_action, index_name, doc_type):
    # bulk_action: [action,source_item,action,source_item,...]
    try:
        es.bulk(bulk_action,index=index_name,doc_type=doc_type,timeout=600)
    except Exception,e: #如果出现错误，就减小存储的批次，再次出现问题的批次直接放弃
        # print 'my_bulk_func Exception: ', str(e)
        for i in range(len(bulk_action)/2):
            temp_bulk_action = bulk_action[2*i : 2*i+2]
            try:
                es.bulk(temp_bulk_action,index=index_name,doc_type=doc_type,timeout=600)
            except:
                pass

def load_date_list(init_flag=False):
    date_list = []
    now_ts = int(time.time())
    if init_flag:   #如果是第一次运行，就加载系统启动至今的数据
        start_ts = datetime2ts(SYSTEM_START_TIME.split(' ')[0])
    else:   #只加载最近7天的数据
        start_ts = datetime2ts(ts2datetime(now_ts-7*24*3600))

    ts = start_ts
    while ts < now_ts:
        date = ts2datetime(ts)
        date_list.append(date)
        ts += 24*3600
    return date_list

def check_data():
    #twitter_user
    if not r.llen(twitter_user_trans_task_name):
        query_body = {
          'post_filter': {
            'missing': {
              'field': 'description_ch'
            }
          },
          'query': {
            'match_all': {
              
            }
          }
        }
        es_scan_results = scan(es,query=query_body,size=1000,index=twitter_user_index_name,doc_type=twitter_user_index_type)
        while True:
            try:
                scan_data = es_scan_results.next()
                item = scan_data['_source']
                r.lpush(twitter_user_trans_task_name, [item['uid'], remove_emoji(item['description']), remove_emoji(item['location'])])
            except StopIteration:
                break
    #twitter_flow_text
    if not r.llen(twitter_flow_text_trans_task_name):
        query_body = {
          'post_filter': {
            'missing': {
              'field': 'text_ch'
            }
          },
          'query': {
            'match_all': {
              
            }
          }
        }
        for date in load_date_list():
            index_name = twitter_flow_text_index_name_pre + date
            es_scan_results = scan(es,query=query_body,size=1000,index=index_name,doc_type=twitter_flow_text_index_type)
            while True:
                try:
                    scan_data = es_scan_results.next()
                    item = scan_data['_source']
                    r.lpush(twitter_flow_text_trans_task_name, [item['tid'], remove_emoji(item['text']), date])
                except StopIteration:
                    break
    #facebook_user
    if not r.llen(facebook_user_trans_task_name):
        query_body = {
          'post_filter': {
            'missing': {
              'field': 'bio_str'
            }
          },
          'query': {
            'match_all': {
              
            }
          }
        }
        es_scan_results = scan(es,query=query_body,size=1000,index=facebook_user_index_name,doc_type=facebook_user_index_type)
        while True:
            try:
                scan_data = es_scan_results.next()
                item = scan_data['_source']
                if item.has_key('description'):
                    description = remove_emoji(item.get('description'))
                else:
                    description = ''
                if item.has_key('quotes'):
                    quotes = remove_emoji(item.get('quotes'))
                else:
                    quotes = ''
                if item.has_key('bio'):
                    bio = remove_emoji(item.get('bio'))
                else:
                    bio = ''
                if item.has_key('about'):
                    about = remove_emoji(item.get('about'))
                else:
                    about = ''  
                r.lpush(facebook_user_trans_task_name, [item['uid'], quotes, bio, about, description])
            except StopIteration:
                break
    #facebook_flow_text
    if not r.llen(facebook_flow_text_trans_task_name):
        query_body = {
          'post_filter': {
            'missing': {
              'field': 'text_ch'
            }
          },
          'query': {
            'match_all': {
              
            }
          }
        }
        for date in load_date_list():
            index_name = facebook_flow_text_index_name_pre + date
            es_scan_results = scan(es,query=query_body,size=1000,index=index_name,doc_type=facebook_flow_text_index_type)
            while True:
                try:
                    scan_data = es_scan_results.next()
                    item = scan_data['_source']
                    r.lpush(facebook_flow_text_trans_task_name, [item['fid'], remove_emoji(item['text']), date])
                except StopIteration:
                    break

def save_fb_user_func(rows):
    facebook_user_mappings()
    bulk_action = []
    count = 0
    user_data = rows
    dict_key_list = ['hometown','location','education','category_list',\
                    'favorite_athletes','interested_in','work','parking',
                    'inspirational_people','languages','cover','favorite_teams']
    if user_data:
        for user in user_data:
            del user['_id'] #删除文档id
            keys_list = user.keys()
            action = {'index':{'_id': user['id']}}
            source_item = {}
            uid = user['id']
            source_item['uid'] = user['id']          
            del user['id']  

            if 'founded' in keys_list:
                try:
                    date_time = int(user['founded'])
                    time_string = str(date_time) + '-01-01 00:00:00'
                    timestamp = int(time.mktime(time.strptime(time_string,"%Y-%m-%d %H:%M:%S")))
                    source_item['founded'] = timestamp
                except:
                    try:
                        if u'年' in user['founded']:
                            import re
                            time_list = re.split(r'[^\u4e00-\u9fa5]',user['founded'])
                            year = str(time_list[0])
                            month = str(time_list[1]).zfill(2)
                            day = str(time_list[2]).zfill(2)
                            date = '-'.join([year,month,day])
                            time_string = date+ ' 00:00:00'
                            timestamp = int(time.mktime(time.strptime(time_string,"%Y-%m-%d %H:%M:%S")))
                            source_item['founded'] = timestamp
                        else:
                            import re
                            time_list = re.split(r'[^\u4e00-\u9fa5]',user['founded'])
                            month = str(time_list[0]).zfill(2)
                            day = str(time_list[1]).zfill(2)
                            year = str(time_list[2])
                            date = '-'.join([year,month,day])
                            time_string = date+ ' 00:00:00'

                            timestamp = int(time.mktime(time.strptime(time_string,"%Y-%m-%d %H:%M:%S")))
                            source_item['founded'] = timestamp
                    except:
                        source_item['founded'] = 0
                del user['founded']

            for dict_key in dict_key_list:
                if dict_key in keys_list:
                    source_item[dict_key] = json.dumps(user[dict_key])
                    del user[dict_key]
            
            if 'updated_time' in keys_list:
                time_string = user['updated_time'][:10] + ' '+user['updated_time'][11:19]
                source_item['update_time'] = int(time.mktime(time.strptime(time_string,"%Y-%m-%d %H:%M:%S")))
                # source_item['update_time'] = int(time.mktime(time.strptime(time_string,"%Y-%m-%d %H:%M:%S"))) - 15*3600
                del user['updated_time']
                
            if 'birthday' in keys_list:
                birthday_list = user['birthday'].split('/')
                try:
                    source_item['birthday'] = '-'.join(birthday_list[:2])
                except:
                    source_item['birthday'] = ' '

                if len(birthday_list) == 3:
                    source_item['birthyear'] = int(birthday_list[2])
                else:
                    source_item['birthyear'] = 0

                del user['birthday']
            keys_list_new = user.keys()
            for key_item in keys_list_new:
                source_item[key_item] = user[key_item]

            #新增属性bio_string（需要翻译），方便画像使用
            #对于长文本，Goslate 会在标点换行等分隔处把文本分拆为若干接近 2000 字节的子文本，再一一查询，最后将翻译结果拼接后返回用户。通过这种方式，Goslate 突破了文本长度的限制。
            if source_item.has_key('description'):
                description = remove_emoji(source_item.get('description')[:1000])  #有的用户描述信息之类的太长了……3000+，没有卵用，而且翻译起来会出现一些问题，截取一部分就行了
            else:
                description = ''
            source_item['description'] = description

            if source_item.has_key('quotes'):
                quotes = remove_emoji(source_item.get('quotes')[:1000])
            else:
                quotes = ''
            source_item['quotes'] = quotes

            if source_item.has_key('bio'):
                bio = remove_emoji(source_item.get('bio')[:1000])
            else:
                bio = ''
            source_item['bio'] = bio

            if source_item.has_key('about'):
                about = remove_emoji(source_item.get('about')[:1000])
            else:
                about = '' 
            source_item['about'] = about 
            #push到redis中，供后续翻译
            r.lpush(facebook_user_trans_task_name, [uid, quotes, bio, about, description])
            
            #把user的uid和nickname映射关系存进redis
            try:
                uname_1 = source_item['username']
                uname_2 = source_item['name']
            except:
                uname_1 = source_item['name']
                uname_2 = source_item['name']
            R_UNAME2ID_FT.hset(fb_uname2id, uname_1, source_item['uid'])
            R_UNAME2ID_FT.hset(fb_uname2id, uname_2, source_item['uid'])

            #bulk
            bulk_action.extend([action,source_item])
            count += 1
            if count % 1000 == 0:
                # es.bulk(bulk_action,index=facebook_user_index_name,doc_type=facebook_user_index_type,timeout=600)
                my_bulk_func(bulk_action, facebook_user_index_name, facebook_user_index_type)
                bulk_action = []
    if bulk_action:
        # es.bulk(bulk_action,index=facebook_user_index_name,doc_type=facebook_user_index_type,timeout=600)
        my_bulk_func(bulk_action, facebook_user_index_name, facebook_user_index_type)


def save_fb_post_func(rows):
    bulk_action_all = {}
    count_dict = {}
    count_i = 0

    chinese_uid_list = load_chinese_user(facebook_user_index_name, facebook_user_index_type)
    
    #[userid, postid, text, createtime, updatetime]
    for row in rows:
        action = {'index':{'_id':row[1]}}
        source_item = {}
        uid = row[0]
        text = row[2][:1000] #注意文本要有长度有上限
        lang = langid.classify(text)[0]
        
        source_item['uid'] = uid
        source_item['fid'] = row[1]
        source_item['text'] = text
        source_item['timestamp'] = row[3]
        # source_item['timestamp'] = row[3] - 15*3600
        source_item['update_time'] = row[4]
        # source_item['update_time'] = row[4] - 15*3600
        date_time = ts2datetime(row[3])
        # date_time = ts2datetime(row[3] - 15*3600)

        #判断uid对应的user是否被标识为发中文帖子的用户
        #从而进行直接存储到es
        #或者存进redis，以便进行翻译处理
        if (uid in chinese_uid_list) or (lang == 'zh'):
            source_item['text_ch'] = traditional2simplified(text)   #进行繁简转换
            source_item['flag_ch'] = 1  #是中文
        else:
            r.lpush(facebook_flow_text_trans_task_name, [row[1], remove_emoji(text), date_time])
            # source_item['flag_ch'] = 0  #非中文
            # 具体是什么文本类型，在翻译的时候进行更新flag_ch字段

        
        #以下部分对中文帖子直接进行bulk存储
        try:
            bulk_action_all[date_time].extend([action,source_item])
            count_dict[date_time] += 1
            count_i += 1
        except:
            bulk_action_all[date_time] = [action,source_item]
            count_dict[date_time] = 1 
            count_i += 1

        for date, count in count_dict.iteritems():
            if count % 1000 == 0 :
                index_name = facebook_flow_text_index_name_pre + date
                facebook_flow_text_mappings(index_name)  # 内含判断
                if bulk_action_all[date]:
                    # es.bulk(bulk_action_all[date],index=index_name,doc_type=facebook_flow_text_index_type,timeout=600)
                    my_bulk_func(bulk_action_all[date], index_name, facebook_flow_text_index_type)
                    bulk_action_all[date] = []
        # if count_i % 1000 == 0:
        #     print count_i

    for date, bulk_action in bulk_action_all.iteritems():
        if bulk_action:
            index_name = facebook_flow_text_index_name_pre + date
            facebook_flow_text_mappings(index_name) # 内含判断
            # es.bulk(bulk_action_all[date],index=index_name,doc_type=facebook_flow_text_index_type,timeout=600)
            my_bulk_func(bulk_action_all[date], index_name, facebook_flow_text_index_type)


def save_fb_count_func(rows):
    bulk_action_all = {}
    count_dict = {}
    count_i = 0

    #id,postid,share,comment,favorite,updatetime
    for row in rows:
        timestamp = int(time.mktime(row[5].timetuple()))
        # timestamp = int(time.mktime(row[5].timetuple())) - 15*3600
        _id = str(row[1]) + '_' + str(timestamp)
        action = {'index':{'_id':_id}}
        source_item = {}
        source_item['uid'] = row[1].split('_')[0]
        source_item['fid'] = row[1].split('_')[1]
        source_item['share'] = row[2]
        source_item['comment'] = row[3]
        source_item['favorite'] = row[4]
        source_item['update_time'] = timestamp

        date_time = ts2datetime(timestamp)

        try:
            bulk_action_all[date_time].extend([action,source_item])
            count_dict[date_time] += 1
            count_i += 1
        except:
            bulk_action_all[date_time] = [action,source_item]
            count_dict[date_time] = 1
            count_i += 1

        for date, count in count_dict.iteritems():
            if count % 1000 == 0:
                index_name = facebook_count_index_name_pre + date
                facebook_count_mappings(index_name)
                if bulk_action_all[date]:
                    # es.bulk(bulk_action_all[date],index=index_name,doc_type=facebook_count_index_type,timeout=600)
                    my_bulk_func(bulk_action_all[date], index_name, facebook_count_index_type)
                    bulk_action_all[date] = []
        # if count_i % 1000 == 0:
        #     print count_i

    for date, bulk_action in bulk_action_all.iteritems():
        if bulk_action:
            index_name = facebook_count_index_name_pre + date
            facebook_count_mappings(index_name)
            # es.bulk(bulk_action_all[date],index=index_name,doc_type=facebook_count_index_type,timeout=600)
            my_bulk_func(bulk_action_all[date], index_name, facebook_count_index_type)



def save_tw_user_func(rows):
    twitter_user_mappings()
    bulk_action = [] 
    count = 0
    for row in rows:
        action = {'index':{'_id':row[1]}}
        source_item = {}
        source_item['uid'] = str(row[1])
        source_item['username'] = row[2]#.decode("utf-8", "replace")
        source_item['userscreenname'] = row[3]#.decode("utf-8", "replace")
        source_item['description'] = row[4][:1000]#.decode("utf-8", "replace")[:1000]
        source_item['create_at'] = int(time.mktime(row[5].timetuple()))
        # source_item['create_at'] = int(time.mktime(row[5].timetuple())) - 15*3600
        try:
            source_item['url'] = row[6]#.decode('utf8')
        except:
            source_item['url'] = ''
        source_item['profile_image_url'] = row[7]
        source_item['profile_background_image_url'] = row[8]
        source_item['location'] = row[9]#.decode("utf-8", "replace")[:1000]
        source_item['timezone'] = row[10]
        source_item['access_level'] = row[11]
        source_item['status_count'] = row[12]
        source_item['followers_count'] = row[13]
        source_item['friends_count'] = row[14]
        source_item['favourites_count'] = row[15]
        source_item['listed_count'] = row[16]
        source_item['is_protected'] = row[17]
        source_item['is_geo_enabled'] = row[18]
        source_item['is_show_all_inline_media'] = row[19]
        source_item['is_contributors_enable'] = row[20]
        source_item['is_follow_requestsent'] = row[21]
        source_item['is_profile_background_tiled'] = row[22]
        source_item['is_profile_use_background_image'] = row[23]
        source_item['is_translator'] = row[24]
        source_item['is_verified'] = row[25]
        source_item['utcoffset'] = row[26]
        source_item['lang'] = row[27]
        source_item['bigger_profile_image_url'] = row[28]
        source_item['bigger_profile_image_url_https'] = row[29]
        source_item['mini_profile_image_url'] = row[30]
        source_item['mini_profile_image_url_https'] = row[31]
        source_item['original_profile_image_url'] = row[32]
        source_item['original_profile_image_url_https'] = row[33]
        source_item['profile_background_image_url_https'] = row[34]
        source_item['profile_banner_ipad_url'] = row[35]
        source_item['profile_banner_ipad_retina_url'] = row[36]
        source_item['profile_banner_mobile_url'] = row[37]
        source_item['profile_banner_mobile_retina_url'] = row[38]
        source_item['profile_banner_retina_url'] = row[39]
        source_item['profile_banner_url'] = row[40]
        source_item['profile_image_url_https'] = row[41]
        source_item['update_time'] = int(time.mktime(row[42].timetuple()))
        # source_item['update_time'] = int(time.mktime(row[42].timetuple())) - 15*3600
        source_item['sensitivity'] = row[43]
        source_item['sensitivity2'] = row[44]

        #push到redis中，供后续翻译
        r.lpush(twitter_user_trans_task_name, [source_item['uid'], remove_emoji(source_item['description']), remove_emoji(source_item['location'])])

        #把user的uid和nickname映射关系存进redis
        R_UNAME2ID_FT.hset(tw_uname2id, source_item['userscreenname'], source_item['uid'])

        bulk_action.extend([action,source_item])
        count += 1
        if count % 1000 == 0:
            # es.bulk(bulk_action,index='twitter_user',doc_type='user',timeout=600)
            my_bulk_func(bulk_action,  twitter_user_index_name, twitter_user_index_type)
            bulk_action = []

    if bulk_action:
        # es.bulk(bulk_action,index='twitter_user',doc_type='user',timeout=600)
        my_bulk_func(bulk_action,  twitter_user_index_name, twitter_user_index_type)



def save_tw_tweet_func(rows):
    bulk_action_all = {}
    count_dict = {}
    count_i = 0

    chinese_uid_list = load_chinese_user(twitter_user_index_name, twitter_user_index_type)
    
    for row in rows:
        action = {'index':{'_id':row[1]}}
        source_item = {}
        tid = str(row[1])
        uid = str(row[2])
        text = row[3][:1000]#.decode("utf-8", "replace")[:1000].encode('utf8') #注意文本要有长度有上限
        lang = langid.classify(text)[0]
        source_item['uid'] = uid
        source_item['tid'] = tid
        source_item['text'] = text
        source_item['timestamp'] = int(time.mktime(row[4].timetuple()))
        # source_item['timestamp'] = int(time.mktime(row[4].timetuple())) - 15*3600
        source_item['update_time'] = int(time.mktime(row[5].timetuple()))
        # source_item['update_time'] = int(time.mktime(row[5].timetuple())) - 15*3600
        date_time = ts2datetime(int(time.mktime(row[4].timetuple())))
        # date_time = ts2datetime(int(time.mktime(row[4].timetuple())) - 15*3600)

        #判断uid对应的user是否被标识为发中文帖子的用户
        #从而进行直接存储到es
        #或者存进redis，以便进行翻译处理
        if (uid in chinese_uid_list) or (lang == 'zh'):
            source_item['text_ch'] = traditional2simplified(text)   #进行繁简转换
            source_item['flag_ch'] = 1  #是中文
        else:
            r.lpush(twitter_flow_text_trans_task_name, [tid, remove_emoji(text), date_time])
            # source_item['flag_ch'] = 0  #非中文
            # 具体是什么文本类型，在翻译的时候进行更新flag_ch字段
            
        #以下部分对中文帖子直接进行bulk存储
        try:
            bulk_action_all[date_time].extend([action,source_item])
            count_dict[date_time] += 1
            count_i += 1
        except:
            bulk_action_all[date_time] = [action,source_item]
            count_dict[date_time] = 1
            count_i += 1

        for date, count in count_dict.iteritems():
            if count % 1000 == 0 :
                index_name = twitter_flow_text_index_name_pre + date
                twitter_flow_text_mappings(index_name)  # 内含判断
                if bulk_action_all[date]:
                    # es.bulk(bulk_action_all[date],index=index_name,doc_type=twitter_flow_text_index_type,timeout=600)
                    my_bulk_func(bulk_action_all[date], index_name, twitter_flow_text_index_type)
                    bulk_action_all[date] = []
        # if count_i % 1000 == 0:
        #     print count_i

    for date, bulk_action in bulk_action_all.iteritems():
        if bulk_action:
            index_name = twitter_flow_text_index_name_pre + date
            twitter_flow_text_mappings(index_name) # 内含判断
            # es.bulk(bulk_action_all[date],index=index_name,doc_type=twitter_flow_text_index_type,timeout=600)
            my_bulk_func(bulk_action_all[date], index_name, twitter_flow_text_index_type)


def save_tw_count_func(rows):
    bulk_action_all = {}
    count_dict = {}
    count_i = 0

    # id,tweetid,share,comment,favorite,updatetime
    for row in rows:
        timestamp = int(time.mktime(row[5].timetuple()))
        # timestamp = int(time.mktime(row[5].timetuple())) - 15*3600
        _id = str(row[1]) + '_' + str(timestamp)
        action = {'index':{'_id':_id}}
        source_item = {}
        source_item['tid'] = str(row[1])
        source_item['share'] = row[2]
        source_item['comment'] = row[3]
        source_item['favorite'] = row[4]
        source_item['update_time'] = timestamp

        date_time = ts2datetime(timestamp)

        try:
            bulk_action_all[date_time].extend([action,source_item])
            count_dict[date_time] += 1
            count_i += 1
        except:
            bulk_action_all[date_time] = [action,source_item]
            count_dict[date_time] = 1
            count_i += 1

        for date, count in count_dict.iteritems():
            if count % 1000 == 0:
                index_name = twitter_count_index_name_pre + date
                twitter_count_mappings(index_name)
                if bulk_action_all[date]:
                    # es.bulk(bulk_action_all[date],index=index_name,doc_type=twitter_count_index_type,timeout=600)
                    my_bulk_func(bulk_action_all[date], index_name, twitter_count_index_type)
                    bulk_action_all[date] = []
        # if count_i % 1000 == 0:
        #     print count_i

    for date, bulk_action in bulk_action_all.iteritems():
        if bulk_action:
            index_name = twitter_count_index_name_pre + date
            twitter_count_mappings(index_name)
            # es.bulk(bulk_action_all[date],index=index_name,doc_type=twitter_count_index_type,timeout=600)
            my_bulk_func(bulk_action_all[date], index_name, twitter_count_index_type)

#user不做createtime的限制 
def save_tw_user():
    base_sql = 'select id,userid,username,userscreenname,description,createat,url,profileimageurl,\
            profilebackgroundimageurl,location,timezone,accesslevel,statuscount,followerscount,\
            friendscount,favouritescount,listedcount,isprotected,isgeoenabled,\
            isshowallinlinemedia,iscontributorsenable,isfollowrequestsent,\
            isprofilebackgroundtiled,isprofileusebackgroundtiled,istranslator,isverified,\
            vtcoffset,lang,biggerprofileimageurl,biggerprofileimageurlhttps,\
            miniprofileimageurl,miniprofileimageurlhttps,originalprofileimageurl,\
            originalprofileimageurlhttps,profilebackgroundimageurlhttps,\
            profilebanneripadurl,profilebanneripadretinaurl,profilebannermobileurl,\
            profilebannermobileretinaurl,profilebannerretinaurl,profilebannerurl,\
            profileimageurlhttps,updatetime,sensitivity,sensitivity2 ' 

    sql_tail = load_sql_time_tail('updatetime')
    # sql_tail_2 = load_sql_time_tail('createat')
    #通用爬虫数据库
    ty_sql = base_sql + 'from user_2017' + ' where ' + sql_tail #+ ' and ' + sql_tail_2
    print 'save_tw_user ty_sql:', ty_sql
    ty_rows = connect_mysqldb(ty_sql, mysql_host, mysql_port, mysql_user, mysql_pwd, 'T2MS_bh', 'user_2017')
    print 'save_tw_user len(ty_rows):', len(ty_rows)
    save_tw_user_func(ty_rows)

    #监听爬虫数据库
    jt_sql = base_sql +' from Tuser' + ' where ' + sql_tail #+ ' and ' + sql_tail_2
    print 'save_tw_user jt_sql:', jt_sql
    jt_rows = connect_mysqldb(jt_sql, mysql_host, mysql_port, mysql_user, mysql_pwd, 'streaming_bh', 'Tuser')
    print 'save_tw_user len(jt_rows):', len(jt_rows)
    save_tw_user_func(jt_rows)

def save_tw_tweet():
    base_sql = 'select id,tweetid,userid,tweettext,createdat,updatetime '
    sql_tail = load_sql_time_tail('updatetime')
    sql_tail_2 = load_sql_time_tail('createdat')

    #通用爬虫数据库
    ty_sql = base_sql + 'from tweet_2017' + ' where ' + sql_tail + ' and ' + sql_tail_2
    print 'save_tw_tweet ty_sql:', ty_sql
    ty_rows = connect_mysqldb(ty_sql, mysql_host, mysql_port, mysql_user, mysql_pwd, 'T2MS_bh', 'tweet_2017')
    print 'save_tw_tweet len(ty_rows):', len(ty_rows)
    save_tw_tweet_func(ty_rows)

    #监听爬虫数据库1
    jt1_sql = base_sql +' from tweet' + ' where ' + sql_tail + ' and ' + sql_tail_2
    print 'save_tw_tweet jt1_sql:', jt1_sql
    jt1_rows = connect_mysqldb(jt1_sql, mysql_host, mysql_port, mysql_user, mysql_pwd, 'streaming_bh', 'tweet')
    print 'save_tw_tweet len(jt1_rows):', len(jt1_rows)
    save_tw_tweet_func(jt1_rows)

    #监听爬虫数据库2
    sql_tail_2 = load_sql_time_tail('createtime')
    jt2_sql = 'select id,tweetid,userid,text,createtime,updatetime from TIDscouting' + ' where ' + sql_tail + ' and ' + sql_tail_2
    print 'save_tw_tweet jt2_sql:', jt2_sql
    jt2_rows = connect_mysqldb(jt2_sql, mysql_host, mysql_port, mysql_user, mysql_pwd, 'streaming_bh', 'TIDscouting')
    print 'save_tw_tweet len(jt2_rows):', len(jt2_rows)
    save_tw_tweet_func(jt2_rows)

def save_tw_count():
    #id,tweetid,share,comment,favorite,updatetime
    sql_tail = load_sql_time_tail('updatetime')

    #监听爬虫数据库1
    jt1_sql = 'select id,tweetid,share,comment,favorite,updatetime from Tscouting' + ' where ' + sql_tail
    print 'save_tw_count jt1_sql:', jt1_sql
    jt1_rows = connect_mysqldb(jt1_sql, mysql_host, mysql_port, mysql_user, mysql_pwd, 'streaming_bh', 'Tscouting')
    print 'save_tw_count len(jt1_rows):', len(jt1_rows)
    save_tw_count_func(jt1_rows)

def save_fb_user():
    query = load_mongodb_time_query(['updated_time'])
    print 'save_fb_user query:', query
    user_data = connect_mongodb(mongodb_host, mongodb_port, 'TsingHuaFBTest_bh', 'user', query=query)
    print 'save_fb_user len(user_data):', len(user_data)
    save_fb_user_func(user_data)


def save_fb_post():
    #[userid, postid, text, createtime, updatetime]

    #通用爬虫数据库TsingHuaFBTest_bh.post
    #db.collection.find(query, {title: 1, by: 1}) // inclusion模式 指定返回的键，不返回其他键
    #'id': '100017447629267_210124696245783',前面为userid，后面为postid
    #但是现在的策略是把100017447629267_210124696245783整个当做postid，前面100017447629267当做userid
    #{u'created_time': u'2018-04-22T05:37:46+0000', u'_id': ObjectId('5aec106e17fec967ff8b3211'), u'id': u'100017447629267_210124696245783', u'updated_time': u'2018-04-22T05:37:46+0000'}
    ty_rows = []
    query_field = {'id':1, 'created_time':1, 'updated_time':1, 'message':1}
    query = load_mongodb_time_query(['updated_time', 'created_time'])
    print 'save_fb_post query:', query
    ty_data = connect_mongodb(mongodb_host, mongodb_port, 'TsingHuaFBTest_bh', 'post', query=query, query_field=query_field)
    for post in ty_data:
        try:
            text = post['message']
            userid = post['id'].split('_')[0]
            postid = post['id']
            createtime = convert_fb_time(post['created_time'])
            updatetime = convert_fb_time(post['updated_time'])
            ty_rows.append([userid, postid, text, createtime, updatetime ])
        except:
            pass
    print 'save_fb_post len(ty_rows):', len(ty_rows)
    save_fb_post_func(ty_rows)
    
    #监听爬虫数据库1
    jt1_rows = []
    query_field = {'id':1, 'created_time':1, 'updated_time':1, 'message':1}
    query = load_mongodb_time_query(['updated_time', 'created_time'])
    print 'save_fb_post query:', query
    jt1_data = connect_mongodb(mongodb_host, mongodb_port, 'FBStreaming_bh', 'post', query=query, query_field=query_field)
    for post in jt1_data:
        try:
            text = post['message']
            userid = post['id'].split('_')[0]
            postid = post['id']
            createtime = convert_fb_time(post['created_time'])
            updatetime = convert_fb_time(post['updated_time'])
            jt1_rows.append([userid, postid, text, createtime, updatetime ])
        except:
            pass
    print 'save_fb_post len(jt1_rows):', len(jt1_rows)
    save_fb_post_func(jt1_rows)
    
    #监听爬虫数据库2
    #postid text createtime updatetime
    #('599672327_10156371636287328', '??? shared a link.', datetime.datetime(2018, 4, 29, 13, 36, 54), datetime.datetime(2018, 5, 4, 16, 59, 21))
    sql_tail = load_sql_time_tail('updatetime')
    sql_tail_2 = load_sql_time_tail('createtime')
    jt2_sql = 'select postid,text,createtime,updatetime from FIDscouting' + ' where ' + sql_tail + ' and ' + sql_tail_2
    print 'save_fb_post jt2_sql:', jt2_sql
    jt2_data = connect_mysqldb(jt2_sql, mysql_host, mysql_port, mysql_user, mysql_pwd, 'streaming_bh', 'FIDscouting')
    jt2_rows = []
    for data in jt2_data:
        userid = data[0].split('_')[0]
        postid = data[0]
        text = data[1]
        createtime = int(time.mktime(data[2].timetuple()))
        updatetime = int(time.mktime(data[3].timetuple()))
        jt2_rows.append([userid, postid, text, createtime, updatetime])
    print 'save_fb_post len(jt2_rows):', len(jt2_rows)
    save_fb_post_func(jt2_rows)
    

def save_fb_count():
    #id,postid,share,comment,favorite,updatetime
    sql_tail = load_sql_time_tail('updatetime')

    #监听爬虫数据库
    sql = 'select id,postid,share,comment,favorite,updatetime from Fscouting' + ' where ' + sql_tail
    print 'save_fb_count sql:', sql
    jt_rows = connect_mysqldb(sql, mysql_host, mysql_port, mysql_user, mysql_pwd, 'streaming_bh', 'Fscouting')
    print 'save_fb_count len(jt_rows):', len(jt_rows)
    save_fb_count_func(jt_rows)

    #通用爬虫数据库TsingHuaFBTest_bh.post
    #db.collection.find(query, {title: 1, by: 1}) // inclusion模式 指定返回的键，不返回其他键
    #'id': '100017447629267_210124696245783',前面为userid，后面为postid
    #{u'created_time': u'2018-04-22T05:37:46+0000', u'_id': ObjectId('5aec106e17fec967ff8b3211'), u'id': u'100017447629267_210124696245783', u'updated_time': u'2018-04-22T05:37:46+0000'}
    ty_rows = []
    query_field = {'id':1, 'shares':1, 'comments':1, 'likes':1, 'updated_time':1}
    query = load_mongodb_time_query(['updated_time'])
    print 'save_fb_count query:', query
    ty_data = connect_mongodb(mongodb_host, mongodb_port, 'TsingHuaFBTest_bh', 'post', query=query, query_field=query_field)
    for post in ty_data:
        try:
            _id = str(post['_id'])
            postid = post['id']
            if post.has_key('shares'):
                share = post['shares']['count']
            else:
                share = 0
            if post.has_key('comments'):
                comment = post['comments']['count']
            else:
                comment = 0
            if post.has_key('likes'):
                favorite = post['likes']['count']
            else:
                favorite = 0
            updatetime = datetime.datetime.fromtimestamp(convert_fb_time(post['updated_time']))
            ty_rows.append([_id, postid, share, comment, favorite, updatetime])
        except:
            pass
    print 'save_fb_count len(ty_rows):', len(ty_rows)
    save_fb_count_func(ty_rows)


def main():
    #不再考虑时差问题，好乱，理不清楚，就这样吧。@hanmc 2018-6-21
    if check_mysqldb():
        create_mappings()
        
        '''
        是否做限制   create_time update_time
        user        ×           √
        flow_text   √           √
        count       ×           √

        '''
        
        #新增数据处理（从清华数据库接过来数据）
        print 'save_tw_user'
        save_tw_user()
        print 'save_tw_tweet'
        save_tw_tweet()
        print 'save_tw_count'
        save_tw_count()

        print 'save_fb_user'
        save_fb_user()
        print 'save_fb_post'
        save_fb_post()
        print 'save_fb_count'
        save_fb_count()

        #遗留数据处理（由于各种原因没能翻译成功的数据再次加入到redis里面，以便再次翻译）
        check_data()
        
        # 清理数据库
        flush_data()

        #更新updatetime
        update_last_ts(dt2)
    else:
        print 'MySQLdb verison Error.'

if __name__ == '__main__':
    main()
