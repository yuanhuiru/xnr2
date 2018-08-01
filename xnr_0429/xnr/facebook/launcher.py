#!/usr/bin/env python
#encoding: utf-8

from selenium import webdriver
import time
import requests
import json
import re
#from pyvirtualdisplay import Display
from selenium.webdriver.firefox.options import Options
#from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

import sys
reload(sys)
sys.setdefaultencoding('utf-8')

class Launcher():
	def __init__(self, username, password):
		self.username = username
		self.password = password

	def login(self):

		options = Options()
		options.add_argument('-headless')
		driver = webdriver.Firefox(firefox_options=options)
		driver.get('https://www.facebook.com')
		print 1111111111
		driver.find_element_by_xpath('//input[@id="email"]').send_keys(self.username)
		print 2222222222
		driver.find_element_by_xpath('//input[@id="pass"]').send_keys(self.password)
		print 3333333333
		driver.find_element_by_xpath('//input[@data-testid="royal_login_button"]').click()
		print 4444444444
		req = requests.Session()
		time.sleep(20)
		# 将cookie保存在req中
		cookies = driver.get_cookies()
		for cookie in cookies:
			req.cookies.set(cookie['name'],cookie['value'])
		headers = {
			'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:56.0) Gecko/20100101 Firefox/56.0'
		}

		return driver



	def login_mobile(self):
		print 'facebook.launcher.login_mobile'
		print 'username', self.username, len(self.username)
		print 'password', self.password, len(self.password)
		options = Options()
		options.add_argument('-headless')
		driver = webdriver.Firefox(firefox_options=options)
		driver.get('https://m.facebook.com/')
		html = driver.page_source
		time.sleep(3)
		with open('login_mobile0000.html', 'w') as f:
			f.write(html)
		print 1111111111
		driver.find_element_by_xpath('//input[@id="m_login_email"]').send_keys(self.username)
		print 2222222222
		driver.find_element_by_xpath('//input[@type="password"]').send_keys(self.password)
		print 3333333333
		driver.find_element_by_xpath('//input[@name="login"]').click()
		print 4444444444
		req = requests.Session()
		time.sleep(3)
		cookies = driver.get_cookies()
		for cookie in cookies:
			req.cookies.set(cookie['name'], cookie['value'])
		headers = {
			'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:56.0) Gecko/20100101 Firefox/56.0'
		}
		try:
			driver.find_element_by_xpath('//div[@id="root"]/table[@role="presentation"]/tbody/tr/td/div/div/a').click()
		except Exception, e:
			print e
			pass
		time.sleep(5)
		#html = driver.page_source
		#with open('launcher.html', 'w') as f:
			#f.write(html)
		return driver


	def get_like_list(self):
		#driver,display = self.login()
		driver = self.login_mobile()
		driver.get('https://m.facebook.com/notifications')
		time.sleep(3)

		#加载更多
		while 1:
			try:
				time.sleep(5)
				driver.find_element_by_xpath('/html/body/div/div/div[2]/div/div[1]/div/div/div[10]/a/span').click()
				continue
			except:
				break

		divs = driver.find_elements_by_xpath('//div[@id="notifications_list"]/div[@id="notifications_list"]/div/div')
		like_list = []
		for div in divs:
			if '赞了你的' in div.text:
				url = div.find_element_by_xpath('./table/tbody/tr/td[2]/a').get_attribute('href')
				like_list.append(url)
		return like_list, driver

	def get_share_list(self):
		#driver,display = self.login()
		driver = self.login_mobile()
		driver.get('https://m.facebook.com/notifications')
		# 退出通知弹窗进入页面
		try:
			driver.find_element_by_xpath('//div[@class="_n8 _3qx uiLayer _3qw"]').click()
		except:
			pass

		time.sleep(3)
		#加载更多
		#length=100
		#for i in range(0,20):
		#	js="var q=document.documentElement.scrollTop="+str(length) 
		#	driver.execute_script(js) 
		#	time.sleep(1)
		#	length+=length

		while 1:
			try:
				time.sleep(5)
				dirver.find_element_by_xpath('/html/body/div/div/div[2]/div/div[1]/div/div/div[10]/a/span').click()
				continue
			except:
				break

		html = driver.page_source
		with open('get_share_list.html', 'wb') as f:
			f.write(html)
		driver.save_screenshot('get_share_list.png')

		divs = driver.find_elements_by_xpath('//div[@id="notifications_list"]/div[@id="notifications_list"]/div/div')
		share_list = []
		for div in divs:
			#data_gt = json.loads(li.get_attribute('data-gt'))
			#type = data_gt['notif_type']
			#if type == "story_reshare":
			if '分享了你的' in div.text:
				url = div.find_element_by_xpath('./table/tbody/tr/td[2]/a').get_attribute('href')
				share_list.append(url)
		#return share_list,driver,display
		return share_list,driver


	def get_mention_list(self):
		#driver,display = self.login()
		driver = self.login()
		driver.get('https://www.facebook.com/notifications')
		# 退出通知弹窗进入页面
		try:
			driver.find_element_by_xpath('//div[@class="_n8 _3qx uiLayer _3qw"]').click()
		except:
			pass

		time.sleep(3)
		#加载更多
		length=100
		for i in range(0,20):
			js="var q=document.documentElement.scrollTop="+str(length)
			driver.execute_script(js)
			time.sleep(1)
			length+=length

		lis = driver.find_elements_by_xpath('//ul[@data-testid="see_all_list"]/li')
		mention_list = []
		for li in lis:
			data_gt = json.loads(li.get_attribute('data-gt'))
			type = data_gt['notif_type']
			if type == "mention" or type == "tagged_with_story":
				url = li.find_element_by_xpath('./div/div/a').get_attribute('href')
				mention_list.append(url)
		#return mention_list,driver,display
		return mention_list,driver

	def get_comment_list(self):
		driver = self.login_mobile()
		#driver,display = self.login()
		#driver.get('https://www.facebook.com/notifications')
		driver.get('https://m.facebook.com/notifications')
		time.sleep(3)

		#加载更多
		while 1:
			try:
				time.sleep(5)
				driver.find_element_by_xpath('/html/body/div/div/div[2]/div/div[1]/div/div/div[10]/a/span').click()
				continue
			except:
				break

		divs = driver.find_elements_by_xpath('//div[@id="notifications_list"]/div[@id="notifications_list"]/div/div')

		comment_list = []
		for div in divs:
			if u'评论了你的' in div.text:
				url = div.find_element_by_xpath('./table/tbody/tr/td[2]/a').get_attribute('href')
				comment_list.append(url)
		return comment_list,driver


	def target_page(self, uid):
		#driver = self.login()
		driver = self.login_mobile()
		#driver,display = self.login()
		#driver.get('https://www.facebook.com/'+uid)
		driver.get('https://m.facebook.com/' + uid + '/')
		time.sleep(3)
		# 退出通知弹窗进入页面
		#try:
		#	driver.find_element_by_xpath('//div[@class="_n8 _3qx uiLayer _3qw"]').click()
		#except:
		#	pass

		return driver
		#return driver,display

	def target_post(self, uid, fid):
		driver = self.login()
		#driver,display = self.login()
		driver.get('https://www.facebook.com/'+uid)		
		time.sleep(3)
		# 退出通知弹窗进入页面
		try:
			driver.find_element_by_xpath('//div[@class="_n8 _3qx uiLayer _3qw"]').click()
		except:
			pass

		#加载更多
		length=100
		for i in range(0,50):
			js="var q=document.documentElement.scrollTop="+str(length) 
			driver.execute_script(js) 
			time.sleep(2)
			length+=length




if __name__ == '__main__':
	launcher = Launcher('8618348831412','qazwsxedcr')
	#print("login start")
	driver = launcher.login()
	#driver,display = launcher.login()
	#launcher.target_page("100011257748826")
	# launcher.target_post('100022568024116')

