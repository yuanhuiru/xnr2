#!/usr/bin/env python
#encoding: utf-8

from launcher import Launcher
import time
from Elasticsearch_fb import Es_fb
import re
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

class Share():
	def __init__(self, username, password):
		self.launcher = Launcher(username, password)
		self.es = Es_fb()
		self.list = []
		self.share_list,self.driver = self.launcher.get_share_list()
		self.update_time = int(time.time())

	def get_share(self):

		for url in self.share_list:
			self.driver.get(url)
			time.sleep(120)
			# 退出通知弹窗进入页面

			try:
				self.driver.find_element_by_xpath('//div[@class="_n8 _3qx uiLayer _3qw"]').click()
			except:
				pass

			page = self.driver.page_source
			self.driver.save_screenshot('get_share000.png')

			#for ea in self.driver.find_elements_by_xpath('//div[@role="feed"]/div'):
			#for ea in divs:
			#	for each in ea.find_elements_by_xpath('./div'):
			try:
				author_name = self.driver.find_element_by_xpath('//table[@role="presentation"]/tbody/tr/td[2]/div/h3/strong/a').text
			except:
				author_name = ''
			print author_name

			try:
				author_id = ''.join(re.search(re.compile('id%3D(\d+)&'), url).group(1))
			except:
				author_id = ''
			print author_id
			#		try:
			#			pic_url = each.find_element_by_xpath('./div[2]/div/div[2]/div/div/a/div/img').get_attribute('src')
			#		except:
			#			pic_url = 'None'


			try:
				content = self.driver.find_element_by_xpath('/html/body/div/div/div[2]/div/div[1]/div[1]/div/div[1]/div[2]').text
			except:
				content = ''


			try:
				timestamp = int(re.search(re.compile('&quot;publish_time&quot;:(\d+),'), page.replace(' ', '').replace('\n', '').replace('\t', '')).group(1))
			except:
				timestamp = ''
			print timestamp

			try:
				mid = ''.join(re.search(re.compile('fbid%3D(\d+)%'), url).group(1))
			except:
				mid = ''
			print mid

			try:
				root_mid = ''.join(re.search(re.compile('&quot;original_content_id&quot;:&quot;(\d+)&quot;'),page).group(1))
			except:
				root_mid = ''
			print root_mid

			try:
				root_text = self.driver.find_element_by_xpath('/html/body/div/div/div[2]/div/div[1]/div[1]/div/div[1]/div[3]/div[2]/div/div/div[2]').text.replace(' ','').replace('\n', '').replace('\t', '')
			except:
				root_text = ''
			print root_text

			item = {'uid':author_id, 'nick_name':author_name, 'mid':mid, 'timestamp':timestamp,\
					 'text':content, 'update_time':self.update_time, 'root_text':root_text, 'root_mid':root_mid}
			self.list.append(item)

		self.driver.quit()
		return self.list
		
	def save(self, indexName, typeName, list):
		self.es.executeES(indexName, typeName, list)

if __name__ == '__main__':
	share = Share('13520874771', '13018119931126731x')
	#share = Share('8618348831412','Z1290605918')
	#share = Share('13041233988','han8528520258')
	list = share.get_share()
	print list
