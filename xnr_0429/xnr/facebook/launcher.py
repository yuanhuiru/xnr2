#!/usr/bin/env python
#encoding: utf-8

from selenium import webdriver
import time
import requests
import json
#from pyvirtualdisplay import Display
from selenium.webdriver.firefox.options import Options
#from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities

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

		options = Options()
		options.add_argument('-headless')
		driver = webdriver.Firefox(firefox_options=options)
		driver.get('https://m.facebook.com/')
		print 1111111111
		driver.find_element_by_xpath('//input[@id="m_login_email"]').send_keys(self.username)
		print 2222222222
		driver.find_element_by_xpath('//input[@type="password"]').send_keys(self.password)
		print 3333333333
		driver.find_element_by_xpath('//input[@name="login"]').click()
		print 4444444444
		req = requests.Session()
		time.sleep(20)
		cookies = driver.get_cookies()
		for cookie in cookies:
			req.cookies.set(cookie['name'], cookie['value'])
		headers = {
			'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:56.0) Gecko/20100101 Firefox/56.0'
		}

		return driver


	def get_like_list(self):
		#driver,display = self.login()
		driver = self.login()
		driver.get('https://www.facebook.com/notifications')
		time.sleep(3)
		# 退出通知弹窗进入页面
		try:
			driver.find_element_by_xpath('//div[@class="_n8 _3qx uiLayer _3qw"]').click()
		except:
			pass

		#加载更多
		length=100
		for i in range(0,20):
			js="var q=document.documentElement.scrollTop="+str(length) 
			driver.execute_script(js) 
			time.sleep(1)
			length+=length

		lis = driver.find_elements_by_xpath('//ul[@data-testid="see_all_list"]/li')
		like_list = []
		for li in lis:
			data_gt = json.loads(li.get_attribute('data-gt'))
			type = data_gt['notif_type']
			if type == "like" or type == "like_tagged" or type == "feedback_reaction_generic":
				url = li.find_element_by_xpath('./div/div/a').get_attribute('href')
				like_list.append(url)
		#return like_list,driver,display
		return like_list, driver

	def get_share_list(self):
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
		share_list = []
		for li in lis:
			data_gt = json.loads(li.get_attribute('data-gt'))
			type = data_gt['notif_type']
			if type == "story_reshare":
				url = li.find_element_by_xpath('./div/div/a').get_attribute('href')
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
		driver = self.login()
		#driver,display = self.login()
		driver.get('https://www.facebook.com/notifications')
		time.sleep(3)
		# 退出通知弹窗进入页面
		try:
			driver.find_element_by_xpath('//div[@class="_n8 _3qx uiLayer _3qw"]').click()
		except:
			pass

		#加载更多
		length=100
		for i in range(0,20):
			js="var q=document.documentElement.scrollTop="+str(length) 
			driver.execute_script(js) 
			time.sleep(1)
			length+=length

		lis = driver.find_elements_by_xpath('//ul[@data-testid="see_all_list"]/li')
		comment_list = []
		for li in lis:
			data_gt = json.loads(li.get_attribute('data-gt'))
			type = data_gt['notif_type']
			if type == "feed_comment":
				url = li.find_element_by_xpath('./div/div/a').get_attribute('href')
				comment_list.append(url)
		#return comment_list,driver,display
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

