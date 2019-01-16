# -*- coding: utf-8 -*-
import sys

import re
import traceback

from tools.Launcher import SinaLauncher

reload(sys)
sys.setdefaultencoding('utf-8')

class SinaOperateAPI:
    def __init__(self, username, password):

        self.launcher = SinaLauncher(username, password)
        self.launcher.login()
        self.uid = self.launcher.uid  # 当前用户id
        self.session = self.launcher.session
        self._headers = {
            "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 9_1 like Mac OS X) AppleWebKit/601.1.46 (KHTML, like Gecko) Version/9.0 Mobile/13B143 Safari/601.1]",
            "Referer": "https://m.weibo.cn",
            'Host': 'm.weibo.cn'
        }

    def publish(self, content):
        if not content:
            return False
        st = re.search(r"st: '(.*?)',", self.session.get('https://m.weibo.cn').text).group(1)
        url = 'https://m.weibo.cn/api/statuses/update'
        post_data = {'content': content.decode('utf-8'), 'st': st}
        resp = self.session.post(url, data=post_data, headers=self._headers)
        if str(resp.status_code) == '200':
            return True, '成功'
        else:
            return False, '失败', resp.status_code

    def retweet(self, mid, content=''):
        st = re.search(r"st: '(.*?)',", self.session.get('https://m.weibo.cn').text).group(1)
        url = 'https://m.weibo.cn/api/statuses/repost'
        post_data = {'id': mid, 'content': content.decode('utf-8'), 'mid': mid, 'st': st}
        resp = self.session.post(url, data=post_data, headers=self._headers)
        if str(resp.status_code) == '200':
            print "retweet----------------------------"
            return True, '成功'
        else:
            return False, '失败', resp.status_code

    def comment(self, mid, content=''):
        st = re.search(r"st: '(.*?)',", self.session.get('https://m.weibo.cn').text).group(1)
        url = 'https://m.weibo.cn/api/comments/create'
        post_data = {'id': mid, 'content': content.decode('utf-8'), 'mid': mid, 'st': st}
        resp = self.session.post(url, data=post_data, headers=self._headers)
        if str(resp.status_code) == '200':
            return True, '成功'
        else:
            return False, '失败', resp.status_code

    #def receive(self, uid, content=''):
    #    st = re.search(r"st: '(.*?)',", self.session.get('https://m.weibo.cn').text).group(1)
    #    url = 'https://m.weibo.cn/api/chat/send'
    #    post_data = {'uid': uid, 'content': content, 'st': st}
    #    resp = self.session.post(url, data=post_data, headers=self._headers)
    #    if str(resp.status_code) == '200':
    #        return True, '成功'
    #    else:
    #        return False, '失败', resp.status_code

    def receive(self, r_mid, mid, content=''):
        st = re.search(r"st: '(.*?)',", self.session.get('https://m.weibo.cn').text).group(1)
        url = 'https://m.weibo.cn/api/comments/reply'
        post_data = {'id': str(r_mid), 'reply': str(mid), 'content': content, 'withReply':'1', 'mid': str(r_mid), 'cid': str(mid), 'st': st}
        resp = self.session.post(url, data=post_data, headers=self._headers)
        if str(resp.status_code) == '200':
            return True, '成功'
        else:
            return False, '失败', resp.status_code

    def privmessage(self, uid, content=''):
        st = re.search(r"st: '(.*?)',", self.session.get('https://m.weibo.cn').text).group(1)
        url = 'https://m.weibo.cn/api/chat/send'
        post_data = {'uid': uid, 'content': content, 'st': st}
        resp = self.session.post(url, data=post_data, headers=self._headers)
        if str(resp.status_code) == '200':
            return True, '成功'
        else:
            return False, '失败', resp.status_code

    def like(self, mid):
        st = re.search(r"st: '(.*?)',", self.session.get('https://m.weibo.cn').text).group(1)
        url = 'https://m.weibo.cn/api/attitudes/create'
        post_data = {'id': mid, 'attitude': 'heart', 'st': st}
        resp = self.session.post(url, data=post_data, headers=self._headers)
        if str(resp.status_code) == '200':
            return True, '成功'
        else:
            return False, '失败', resp.status_code

    def unlike(self, mid):
        st = re.search(r"st: '(.*?)',", self.session.get('https://m.weibo.cn').text).group(1)
        url = 'https://m.weibo.cn/api/attitudes/destroy'
        post_data = {'id': mid, 'attitude': 'heart', 'st': st}
        resp = self.session.post(url, data=post_data, headers=self._headers)
        if str(resp.status_code) == '200':
            return True, '成功'
        else:
            return False, '失败', resp.status_code

    def followed(self, uid):
        st = re.search(r"st: '(.*?)',", self.session.get('https://m.weibo.cn').text).group(1)
        url = 'https://m.weibo.cn/api/friendships/create'
        post_data = {'uid': uid, 'st': st}
        resp = self.session.post(url, data=post_data, headers=self._headers)
        if str(resp.status_code) == '200':
            return True, '成功'
        else:
            return False, '失败', resp.status_code

    def unfollowed(self, uid):
        st = re.search(r"st: '(.*?)',", self.session.get('https://m.weibo.cn').text).group(1)
        url = 'https://m.weibo.cn/api/friendships/destory'
        post_data = {'uid': uid, 'st': st}
        resp = self.session.post(url, data=post_data, headers=self._headers)
        if str(resp.status_code) == '200':
            return True, '成功'
        else:
            return False, '失败', resp.status_code

def weibo_publish_main(username, password, text='', file=''):
    try:
        sina_operate_api = SinaOperateAPI(username, password)
        sina_operate_api.publish(content=text)
        mark = True
    except Exception, e:
        traceback.print_exc(e)
        mark = False
    return mark


if __name__ == '__main__':

    sina_operate_api = SinaOperateAPI('13269704912', 'murcielagolp640')
    # print sina_operate_api.publish(content='hhhh')
    # print sina_operate_api.retweet(mid='', content='')
    print sina_operate_api.receive(mid='4305918151652871', r_mid='4301849861467864',  content='hhhhh')
    # print sina_operate_api.comment(mid='4301849861467864', content='测试')
    # print sina_operate_api.like(mid='')
    # print sina_operate_api.followed(uid='')
    # print sina_operate_api.unfollowed(uid='')
    # print sina_operate_api.privmessage(uid='5393019099', content='hello')

