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


class FeedbackAt:
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

    def atMeComments(self):
        json_list = []
        tags = False
        page = 1
        while True:
            at_url = 'https://m.weibo.cn/message/mentionsCmt?page={}'.format(page)
            #at_url = 'https://m.weibo.cn/message/mentionsAt?page={}'.format(page)
            print at_url
            resp = self.session.get(at_url)
            if page == 2:
                print '++++++++++++++++++++++++++', resp.json()['data']
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
                timestamp = int(time.mktime(time.strptime(self.read_datetime(data['status']['created_at']), '%Y-%m-%d %H:%M:%S')))
                text = re.sub(r'<.*?>', r'', data['status']['text'])
                print text
                root_mid = data['status']['mid']
                root_uid = data['status']['user']['id']
                _type = 'stranger'
                type1 = 'followed' if data['user']['following'] is True else ''
                type2 = 'follow' if data['user']['follow_me'] is True else ''
                weibo_type = 'friend' if type1 and type2 else _type
                update_time = int(time.time())
                item = {
                    'photo_url': photo_url,
                    'uid': uid,
                    'nick_name': nickname,
                    'mid': mid,
                    'timestamp': timestamp,
                    'text': text,
                    'root_mid': root_mid,
                    'root_uid': self.uid,
                    'weibo_type': weibo_type,
                    'update_time': update_time
                }
                print json.dumps(item, ensure_ascii=False)
                json_list.append(json.dumps(item, ensure_ascii=False))
            page += 1
        return json_list

    def execute(self):
        comments = self.atMeComments()
        #print 111
        #executeES('weibo_feedback_at', 'text', comments)
        #print 222

if __name__ == '__main__':

    feedback_at = FeedbackAt('18737028295', 'xuanhui99999')
    # print feedback_at.atMeComments()
    feedback_at.execute()
