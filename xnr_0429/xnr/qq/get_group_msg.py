# -*- coding: utf-8 -*-

import redis
import json
import hashlib
import datetime

import sys
sys.path.append('../')

from global_utils import *
from sensitive_compute import sensitive_check
from qq_xnr_groupmessage_mappings import group_message_mappings

QUEUE = "QQGroupMsg"
redisPool = redis.ConnectionPool(host='localhost', port=6003)
client = redis.Redis(connection_pool=redisPool)

def get_group_msg():
    QUEUE = "QQGroupMsg"
    redisPool = redis.ConnectionPool(host='localhost', port=6003)
    client = redis.Redis(connection_pool=redisPool)
    while True:
        group_msg_data = json.loads(client.brpop(QUEUE)[1])
        msg_list = group_msg_data['message']
        print group_msg_data
        for msg in msg_list:
            msg_type = msg['type']
            if msg_type == 'text':
                xnr_qq_number = group_msg_data['uid']
                xnr_nickname = group_msg_data['user_name']
                timestamp = group_msg_data['receive_time_ts']
                speaker_qq_number = group_msg_data['sender']['user_id']
                text = msg['data']['text']
                # sen_value, sen_words = sensitive_check(text)
                #if sen_value !=0:
                #    sen_flag = 1    #该条信息是敏感信息
                #else:
                #    sen_flag = 0
                speaker_nickname = group_msg_data['sender']['nickname']
                qq_group_number = group_msg_data['group_id']
                qq_group_nickname = group_msg_data['group_name']

                group_msg_item = {
                    'xnr_qq_number': xnr_qq_number,
                    'xnr_nickname': xnr_nickname,
                    'timestamp': timestamp,
                    'speaker_qq_number': speaker_qq_number,
                    'text': text,
                    'sensitive_flag': 0,
                    'sensitive_value': '',
                    'sensitive_words_string': '',
                    'speaker_nickname': speaker_nickname,
                    'qq_group_number': qq_group_number,
                    'qq_group_nickname': qq_group_nickname
                }

                qq_json = json.dumps(group_msg_item)
                print 'qq_json=====:',qq_json
                #conMD5 = string_md5(text)

                nowDate = datetime.datetime.now().strftime('%Y-%m-%d')
                index_name = group_message_index_name_pre+ str(nowDate)
                print 'INDEX NAME-=-------------=-=-'
                print index_name
                # 让系统随机分配 _id
                if not es_xnr.indices.exists(index=index_name):
                    print 'get mapping'
                print es_xnr.index(index=index_name, doc_type=group_message_index_type,body=group_msg_item)
        
    

if __name__ == '__main__':
    get_group_msg()
