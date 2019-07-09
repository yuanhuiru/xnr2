# -*- coding: utf-8 -*-
import json
import sys

import re
import time
import traceback

from tools.Launcher import SinaLauncher

reload(sys)
sys.setdefaultencoding('utf-8')


def get_user_info_by_uid(username, password, uid):
    launcher = SinaLauncher(username, password)
    launcher.login()
    session = launcher.session
    url = 'https://m.weibo.cn/api/container/getIndex?uid={uid}&luicode=10000011&lfid=231018{uid}_-_longbloglist_original&type=uid&value={uid}&containerid=100505{uid}'.format(uid=str(uid))

    userInfo = json.loads(session.get(url=url).text)['data']['userInfo']

    uid = userInfo['id']
    screen_name = userInfo.get('screen_name', '')
    follow_count = userInfo.get('follow_count', 0)
    followers_count = userInfo.get('followers_count', 0)
    statuses_count = userInfo.get('statuses_count', 0)
    description = userInfo.get('description', '')
    profile_image_url = userInfo.get('profile_image_url', '')
    gender = userInfo.get('gender', '')
    profile_url = userInfo.get('profile_url', '')
    verified_reason = userInfo.get('verified_reason', '')

    item = {
        'uid': uid,
        'screen_name': screen_name,
        'follow_count': follow_count,
        'followers_count': followers_count,
        'statuses_count': statuses_count,
        'description': description,
        'profile_image_url': profile_image_url,
        'gender': gender,
        'profile_url': profile_url,
        'verified_reason': verified_reason
    }

    return item



if __name__ == '__main__':
    username = '13269704912'
    password = 'murcielagolp640'
    userinfo_item = get_user_info_by_uid(username=username, password=password, uid=1738932247)
    print userinfo_item

