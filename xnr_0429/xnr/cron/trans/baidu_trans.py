#/usr/bin/env python
#coding=utf8
 
import httplib
import md5
import urllib
import random
import json
import langid
#from trans import traditional2simplified
from langconv import *

appid = '20181119000236501' #你的appid
secretKey = 'ptHQmHhGnwy0hkWYGjAt' #你的密钥

#繁体转简体
def traditional2simplified(sentence):
    '''
    将sentence中的繁体字转为简体字
    :param sentence: 待转换的句子
    :return: 将句子中繁体字转换为简体字之后的句子
    '''
    sentence = Converter('zh-hans').convert(sentence)
    return sentence

def translate(q):
    #if langid.classify(q)[0] == 'zh':
    #    return traditional2simplified(q)

    httpClient = None
    myurl = '/api/trans/vip/translate'
    fromLang = 'auto'
    toLang = 'zh'
    salt = random.randint(32768, 65536)

    sign = appid+q+str(salt)+secretKey
    m1 = md5.new()
    m1.update(sign)
    sign = m1.hexdigest()
    #s = [u'\u71c8\u7c60\u9001\u540c\u4e8b\u4e86 \u54c7\uff01           '][0].encode('utf8')
    
    try:
        q = q.encode('utf8')
    except:
        pass
    if langid.classify(q)[0] == 'zh':
        return traditional2simplified(q.decode('utf8'))



    #print [s], [q]
    q = urllib.quote(q)
    #print q
    myurl = myurl+'?appid='+appid+'&q='+q+'&from='+fromLang+'&to='+toLang+'&salt='+str(salt)+'&sign='+sign
     
    try:
        httpClient = httplib.HTTPConnection('api.fanyi.baidu.com')
        httpClient.request('GET', myurl)
     
        #response是HTTPResponse对象
        response = httpClient.getresponse()
        result = json.loads(response.read())
        return result['trans_result'][0]['dst']
    except Exception, e:
        print e
        return False
    finally:
        if httpClient:
            httpClient.close()

if __name__ == '__main__':
    print translate('The kids who can go and drink with dad are all good kids.❤️❤️❤️')
    print translate("「只有弱者才會逞強，只有強者才懂示弱 。 刻薄是因為底子薄，尖酸是因為心裡酸 ")
