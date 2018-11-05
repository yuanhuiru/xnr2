# coding=utf-8

import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import json
import time
import re
import os
import codecs
import shutil
import urllib
import random
from urllib import quote
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.firefox.options import Options
import selenium.webdriver.support.ui as ui
from selenium.webdriver.common.action_chains import ActionChains
from BeautifulSoup import BeautifulSoup
import random
#from pyvirtualdisplay import Display
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities

from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

import traceback

source_list = [
"http://widget.weibo.com/dialog/PublishWeb.php?refer=y&app_src=3o33sO&button=pubilish", # 发布窗
"http://widget.weibo.com/dialog/PublishWeb.php?refer=y&app_src=5yrCy2&button=pubilish", # 爱尖刀科技媒体
"http://widget.weibo.com/dialog/PublishWeb.php?refer=y&app_src=19mGfK&button=%E4%BD%9C%E8%19mGfK80%85@laenix", # Nexus6
]

def login_m_weibo_cn(username, password):

    options = Options()
    options.add_argument('-headless')  # 无头参数
    driver = webdriver.Firefox(firefox_options=options)
    try:
        print u'准备登陆Weibo.cn网站...'
        driver.get("https://passport.weibo.cn/signin/login?entry=mweibo&r=http%3A%2F%2Fm.weibo.cn")
        # sleep 以应对没出现相应元素
        time.sleep(5)
        driver.find_element_by_id("loginName").clear()
        driver.find_element_by_id("loginName").send_keys(username) #用户名
        driver.find_element_by_id("loginPassword").clear()
        driver.find_element_by_id("loginPassword").send_keys(password)  #密码
        driver.find_element_by_id("loginAction").send_keys(Keys.RETURN)

        # sleep以应对验证码
        time.sleep(10)
        try:
            driver.find_element_by_xpath('//a[@id="getCode"]').click()
            code = str(input('请输入验证码：'))
            time.sleep(5)
            driver.find_element_by_xpath('//input[@id="checkCode"]').send_keys(code)
            driver.find_element_by_xpath('//a[@id="submitBtn"]').click()
        except Exception, e:
            traceback.print_exc(e)
        driver.save_screenshot('11111.png')
        return driver

    except Exception, e:
        traceback.print_exc(e)
        #driver.quit()

if __name__ == '__main__':
    username = 'magnificenthill@sina.com'
    password = 'yunzhonghaihai92'
    login_m_weibo_cn(username, password)
