#-*- coding: utf-8 -*-

'''
不需要翻墙也能用的google翻译
原理是破解https://translate.google.cn/的请求参数，直接请求翻译
使用包GoogleFreeTrans参见：https://github.com/ziliwang/GoogleFreeTrans
值得注意的是，该包默认js的运行环境是PhantomJS，对于ubuntu而言可能需要更换成不需要display的环境，比如: Node
更换js运行环境为Node，参见: 
    python运行js: https://www.jianshu.com/p/e01a3d504700
    Node.js 安装配置: http://www.runoob.com/nodejs/nodejs-install-setup.html
'''
import os, execjs
from GoogleFreeTrans import Translator as TS
import langid
os.environ["EXECJS_RUNTIME"] = "Node"


class TransRes():
    def __init__(self, src, text):
        self.src = src
        self.text = text

class Translator():
    def langdetect(self, q):
        return langid.classify(q)[0]

    def trans(self, q):
        try:
            translator = TS.translator(src='auto', dest=self.toLang)
            text = translator.translate(q)
            return text
        except Exception,e:
            print 'google_trans_v2 Exception: ', str(e)
            return False

    def translate(self, q, target_language='zh-cn', test_num=2):
        if target_language == 'zh-cn':
            self.toLang = 'zh-CN'
        elif target_language == 'en':
            self.toLang = 'en'

        src = self.langdetect(q)
        if src == 'zh':
            text = q
        else:
            while test_num:
                text = self.trans(q)
                if text:
                    break
                test_num = test_num - 1
            if not text:
                text = q
        
        return TransRes(src, text)

if __name__ == '__main__':
    q = "‏‏‏‏‏‏‏‏‏‏‏‏‏‏‏‏‏‏‏‏‏‏‏‏‏‏‏‏‏‏‏‏‏‏‏‏‏طالبة جامعية ‏‏من مواليد [١٩٩١م] طموحي الحصول على الدكتوراه في تخصصي من هواياتي: كتابه الخط العربي وعشقي مايسمى [بالتصوير"
    q = '你好啊'
    q = u'\u30b9\u30de\u30dbRPG\u306f\u4eca\u3053\u308c\u3092\u3084\u3063\u3066\u308b\u3088\u3002\u4eca\u306f\u3053\u306e\u30a4\u30d9\u30f3\u30c8\u304c\u958b\u50ac\u4e2d\uff01\u3000\u2192\u3000https://t.co/Fhdz3QQ3OI https://t.co/Jo2iC4w5OB'
    q = q.encode('utf8')
    r = Translator().translate(q)
    print r.text, r.src

