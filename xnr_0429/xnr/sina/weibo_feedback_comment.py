# -*- coding: utf-8 -*-
import json
import sys

import re
import time
import traceback

from tools.Launcher import SinaLauncher
from tools.ElasticsearchJson import executeES

reload(sys)
sys.setdefaultencoding('utf-8')

class FeedbackComment:
    def __init__(self, username, password):
        self.launcher = SinaLauncher(username, password)
        self.launcher.login()
        self.uid = self.launcher.uid
        self.session = self.launcher.session

    @staticmethod
    def read_datetime(raw_datatime):
        datetime_splited_list = raw_datatime.split(' ')
        month = str(datetime_splited_list[1])
        month = [i.values()[0] for i in
                 [{'Jan': '01'}, {'Feb': '02'}, {'Mar': '03'}, {'Apr': '04'}, {'May': '05'}, {'Jun': '06'},
                  {'Jul': '07'}, {'Aug': '08'}, {'Sep': '09'}, {'Oct': '10'}, {'Nov': '11'}, {'Dec': '12'}] if
                month == i.keys()[0]][0]
        day = datetime_splited_list[2]
        time = datetime_splited_list[3]
        year = datetime_splited_list[5]
        datetime = year + '-' + month + '-' + day + ' ' + time
        return datetime

    def commentInbox(self):
        json_list = []
        page = 1
        while True:
            comment_inbox_url = 'https://m.weibo.cn/message/cmt?page={}'.format(page)
            resp = self.session.get(comment_inbox_url)
            try:
                resp.json()['data']
            except:
                break
            if resp.json()['data'] is False:
                break
            else:
                pass
            data_list = resp.json()['data']
            for data in data_list:
                photo_url = data['user']['profile_image_url']
                uid = data['user']['id']
                nickname = data['user']['screen_name']
                mid = data['mid']
                timestamp = int(time.mktime(time.strptime(self.read_datetime(data['created_at']), '%Y-%m-%d %H:%M:%S')))
                text = re.sub(r'<.*?>', r'', data['text'])
                try:
                    root_mid = data['status']['mid']
                except:
                    continue
                root_uid = data['status']['user']['id']
                _type = 'stranger'
                type1 = 'followed' if data['user']['following'] is True else ''
                type2 = 'follow' if data['user']['follow_me'] is True else ''
                weibo_type = 'friend' if type1 and type2 else _type
                update_time = int(time.time())
                item = {
                    'photo_url': photo_url,
                    'uid': str(uid),
                    'nick_name': nickname,
                    'mid': str(mid),
                    'timestamp': timestamp,
                    'text': text,
                    'root_mid': str(root_mid),
                    'root_uid': str(root_uid),
                    'weibo_type': weibo_type,
                    'comment_type': 'receive',
                    'update_time': update_time
                }
                json_list.append(json.dumps(item, ensure_ascii=False))
            page += 1
        return json_list

    def commentOutbox(self):
        json_list = []
        page = 1
        while True:
            comment_inbox_url = 'https://m.weibo.cn/message/myCmt?page={}'.format(page)
            resp = self.session.get(comment_inbox_url)
            try:
                resp.json()['data']
            except:
                break
            if resp.json()['data'] is False:
                break
            else:
                pass
            data_list = resp.json()['data']
            for data in data_list:
                try:
                    data['status']
                except:
                    continue
                photo_url = data['user']['profile_image_url']
                uid = data['user']['id']
                nickname = data['user']['screen_name']
                mid = data['mid']
                timestamp = int(time.mktime(time.strptime(self.read_datetime(data['created_at']), '%Y-%m-%d %H:%M:%S')))
                text = re.sub(r'<.*?>', r'', data['text'])
                root_mid = data['status']['mid']
                root_uid = data['status']['user']['id']
                _type = 'stranger'
                type1 = 'followed' if data['user']['following'] is True else ''
                type2 = 'follow' if data['user']['follow_me'] is True else ''
                weibo_type = 'friend' if type1 and type2 else _type
                update_time = int(time.time())
                item = {
                    'photo_url': photo_url,
                    'uid': str(uid),
                    'nick_name': nickname,
                    'mid': str(mid),
                    'timestamp': timestamp,
                    'text': text,
                    'root_mid': str(root_mid),
                    'root_uid': str(root_uid),
                    'weibo_type': weibo_type,
                    'comment_type': 'make',
                    'update_time': update_time
                }
                json_list.append(json.dumps(item, ensure_ascii=False))
            page += 1
        #print json_list
        return json_list

    def execute(self):
        comment_inbox = self.commentInbox()
        print "comment to es ============================= "
        #print comment_inbox
        executeES('weibo_feedback_comment', 'text', comment_inbox)
        #comment_outbox = self.commentOutbox()
        #print "comment to es ============================= "
        #executeES('weibo_feedback_comment', 'text', comment_outbox)


if __name__ == '__main__':
    #feedback_comment = FeedbackComment('13269704912', 'murcielagolp640')
    feedback_comment = FeedbackComment('18737028295', 'xuanhui99999')
    #print feedback_comment.commentInbox()
    #print feedback_comment.commentOutbox()
    #feedback_comment.commentOutbox()
    feedback_comment.execute()

