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

class FeedbackRetweet:
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

    def atMeMicroBlog(self):
        json_list = []
        page = 1
        while True:
            at_me_microblog_url = 'https://m.weibo.cn/message/mentionsAt?page={}'.format(page)
            resp = self.session.get(at_me_microblog_url)
            data_list = resp.json()['data']
            if not data_list:
                break
            else:
                pass
            for data in data_list:
                if data.has_key('retweeted_status'):
                    try:
                        data['retweeted_status']['user']['id']
                    except:
                        continue
                    if int(data['retweeted_status']['user']['id']) == int(self.uid):
                        _type = 'stranger'
                        type1 = 'followed' if data['user']['following'] is True else ''
                        type2 = 'follow' if data['user']['follow_me'] is True else ''
                        photo_url = data['user']['profile_image_url']
                        uid = data['user']['id']
                        nick_name = data['user']['screen_name']
                        mid = data['mid']
                        timestamp = int(time.mktime(time.strptime(self.read_datetime(data['created_at']), '%Y-%m-%d %H:%M:%S')))
                        text = re.sub(r'<.*?>', r'', data['text'])
                        retweet = data['retweeted_status']['reposts_count']
                        comment = data['comments_count']
                        root_mid = data['retweeted_status']['mid']
                        root_uid = self.uid
                        weibo_type = 'friend' if type1 and type2 else _type
                        update_time = int(time.time())
                        item = {
                            'photo_url': photo_url,
                            'uid': str(uid),
                            'nick_name': nick_name,
                            'mid': str(mid),
                            'timestamp': timestamp,
                            'text': text,
                            'retweet': retweet,
                            'comment': comment,
                            'root_mid': str(root_mid),
                            'root_uid': str(root_uid),
                            'weibo_type': weibo_type,
                            'update_time': update_time
                        }
                        json_list.append(json.dumps(item, ensure_ascii=False))
            page += 1
        return json_list

    def excute(self):
        retweet = self.atMeMicroBlog()
        executeES('weibo_feedback_retweet', 'text', retweet)


if __name__ == '__main__':
    #weibo_feedback_retweet = FeedbackRetweet('13269704912', 'murcielagolp640')
    weibo_feedback_retweet = FeedbackRetweet('18737028295', 'xuanhui99999')
    # print weibo_feedback_retweet.atMeMicroBlog()
    weibo_feedback_retweet.excute()

