#!/usr/bin/env python
#encoding: utf-8

from launcher import Launcher
import time
from Elasticsearch_fb import Es_fb
import re

class Comment():
	def __init__(self, username, password):
		self.launcher = Launcher(username, password)
		self.es = Es_fb()
		self.comment_list,self.driver = self.launcher.get_comment_list()
		self.list = []
		self.update_time = int(time.time())

	def date2timestamp(self, date):
		date = date.replace(u'月', '-').replace(u'日', '')
		if u'年' not in date:
			date = str(time.strftime('%Y-%m-%d', time.localtime(time.time())).split('-')[0]) + '-' + date
		else:
			date = date.replace(u'年', '-')
		timestamp = int(time.mktime(time.strptime(date, '%Y-%m-%d')))
		return timestamp

	def get_comment(self):
		print 'comment_list', self.comment_list
		for url in self.comment_list:
			self.driver.get(url)
			time.sleep(1)


			try:
				root_text = self.driver.find_element_by_xpath('//div[@id="m_story_permalink_view"]/div[1]/div/div[1]/div[2]').text
			except BaseException, e:
				root_text = 'None'

			
			try:
				root_mid = ''.join(re.search(re.compile('fbid%3D(\d+)%'),url).group(1))
				print 'root_mid', root_mid
			except BaseException, e:
				print "get_comment Position44444444", e 
				root_mid = ''

			for each in self.driver.find_elements_by_xpath('//div[@id="m_story_permalink_view"]/div[2]/div/div[4]/div'):
				if u' 查看更多评论' in each.text:
					break

				try:
					author_name = each.find_element_by_xpath('./div/h3/a').text
				except BaseException, e:
					print "get_comment Position66666666", e
					try:
						author_name = each.find_element_by_xpath('./div[1]/div/div/div[2]/div/div/div/div[1]/div/span/span[1]/a').text
					except:
						author_name = ''
				print author_name



				
				try:
					print 6666666666
					author_id = ''.join(re.findall(re.compile('id=(\d+)'),each.find_element_by_xpath('./div/h3/a').get_attribute('href')))
				except:
					author_id = ''					

				#try:
				#	print 7777777777
				#	pic_url = each.find_element_by_xpath('./div/div/div/div[1]/a/img').get_attribute('src')
				#except:
				#	pic_url = 'None'
				try:
					print 888888888
					content = each.find_element_by_xpath('./div/div[1]').text
				except:
					try:
						content = each.find_element_by_xpath('./div/div[1]/span/span').text
					except:
						content = 'Emoji'
				print content

				try:
					print 99999999
					ti = self.date2timestamp(str(each.text.replace(' ', '').split('·')[5]))
				except:
					ti = self.date2timestamp(str(each.text.replace(' ', '').split('·')[4]))					
				self.list.append({'uid':author_id, 'nick_name':author_name, 'mid':root_mid, 'timestamp':ti, 'text':content,\
										 'update_time':self.update_time, 'root_text':root_text, 'root_mid':root_mid})

		self.driver.quit()
		return self.list

	def save(self,indexName,typeName,list):
		self.es.executeES(indexName, typeName, list)

if __name__ == '__main__':
	# comment = Comment('8618348831412','Z1290605918')
	comment = Comment('8613520874771','13018119931126731x')
	#comment = Comment('+8613269704912', 'chenhuiping')
	list = comment.get_comment()
	print list
	#self.display.popen.kill()
