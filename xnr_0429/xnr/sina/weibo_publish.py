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
    try:
        options = Options()
        options.add_argument('-headless')  # 无头参数
        driver = webdriver.Firefox(firefox_options=options)
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
        driver.save_screenshot('11111.png')
        return driver
    except Exception, e:
        traceback.print_exc(e)
        driver.quit()

def publish_by_source(text, driver):
    try:
        wait = WebDriverWait(driver, 15)
        #url = random.choice(source_list)
        url = source_list[2]
        print url
        while 1:
            driver.get(url)
            try:
                print 'texta1'
                texta = driver.find_element_by_xpath("/html/body/div[1]/div/div[2]/div[1]/div/div[2]/textarea")
            except:
                print 'texta2'
                texta = driver.find_element_by_xpath("/html/body/section[1]/section/section[1]/div/div[2]/textarea")
            texta.send_keys(text)
            time.sleep(5)
            try:
                print 'driver1'
                driver.find_element_by_xpath("/html/body/div[1]/div/div[2]/div[1]/div/div[3]/div[2]/a").click()
                print '------'
            except:
                print 'driver2'
                driver.find_element_by_xpath("//*[@id=\"pl_publish_publishMobile\"]/section/section[1]/div/div[3]/div/a[3]").click()
            result_p = wait.until(
                EC.presence_of_element_located((By.XPATH, '//p[@class="result_declare"]'))
            )
            if result_p.text == '发布失败':
                continue
            else:
                break
    
        print driver.page_source
        print "publish_by_source Success!!"
        driver.quit()
    except Exception, e:
        traceback.print_exc(e)
        driver.save_screenshot('weibo.png')
        driver.quit()
def publish_by_source_with_picture(text, file, driver):
    try:
        #url = random.choice(source_list)
        url = source_list[2]
        driver.get(url)
        try:
            texta = driver.find_element_by_xpath("/html/body/div[1]/div/div[2]/div[1]/div/div[2]/textarea")
        except:
            texta = driver.find_element_by_xpath("/html/body/section[1]/section/section[1]/div/div[2]/textarea")
        texta.send_keys(text)
        time.sleep(1)
        driver.find_element_by_xpath('//a[@title="图片"]').click()
        time.sleep(1)
        driver.find_element_by_xpath('//input[@id="activity_img"]').send_keys(file)
        time.sleep(1)
        try:
            driver.find_element_by_xpath("/html/body/div[1]/div/div[2]/div[1]/div/div[3]/div[2]/a").click()
        except:
            driver.find_element_by_xpath("//*[@id=\"pl_publish_publishMobile\"]/section/section[1]/div/div[3]/div/a[3]").click()
        driver.quit()
    except:
        driver.quit()


def weibo_publish(username,password,text):
    driver = login_m_weibo_cn(username, password)
    driver.save_screenshot('weibo_public_position1111.png')
    try:
        publish_by_source(text, driver)
        driver.quit()
    except Exception, e:
        traceback.print_exc(e)
        driver.quit()

def weibo_publish_with_picture(username,password,text,file):
    driver = login_m_weibo_cn(username, password)
    try:
        publish_by_source_with_picture(text, file, driver)
        driver.quit()
    except:
        driver.quit()

def weibo_publish_main(username,password,text,file=''):
    try:
        if file:
            weibo_publish_with_picture(username,password,text,file)
        else:
            weibo_publish(username,password,text)

        mark = True
    except Exception, e:
        traceback.print_exc(e)
        mark = False
    print mark
    return mark


if __name__ == '__main__':
    #定义变量
    username = 'magnificenthill@sina.com' #输入你的用户名
    password = 'yunzhonghaihai92' #输入你的密码
    #username = '80617252@qq.com' #输入你的用户名
    #password = 'xuanhui99999' #输入你的密码

    text = '测试oook'.decode('utf-8')

    #file = '/home/xnr1/xnr_0429/xnr/static/images/icon.png'
    weibo_publish_main(username,password,text)
    
    #if file:
    #    weibo_publish_with_picture(username,password,text,file)
    #else:
    #    weibo_publish(username,password,text)
    #login_m_weibo_cn(username, password)
    
