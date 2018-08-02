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
		date = date.replace(u'月', '-').replace(u'日', '').replace(' ', '')
		if date == '刚刚':
			timestamp = int(time.time())
			return timestamp
		if u'上午' in date:
			date = date.replace(u'上午', ' ')
		if u'下午' in date:
			if date.split(u'下午')[1].split(':')[0] == '12':
				date = date.replace(u'下午', ' ')
			elif eval(date.split(u'下午')[1].split(':')[0]) < 12:
				date = date.split(u'下午')[0] + ' ' + str(eval(date.split(u'下午')[1].split(':')[0])+12) + ':' + date.split(u'下午')[1].split(':')[1]
		if u'年' not in date and u'分钟' not in date and u'小时' not in date: 
			date = str(time.strftime('%Y-%m-%d', time.localtime(time.time())).split('-')[0]) + '-' + date 
		if u'年' in date and u'分钟' not in date and u'小时' not in date: 
			date = date.replace(u'年', '-') 

		if u'分钟' in date: 
			timestamp = int(time.time()) - int(re.search(r'(\d+)', date).group(1)) * 60 
			return timestamp 
		if u'小时' in date: 
			timestamp = int(time.time()) - int(re.search(r'(\d+)', date).group(1)) * 60 * 60 
			return timestamp 

		try: 
			timestamp = int(time.mktime(time.strptime(date, '%Y-%m-%d'))) 
		except: 
			timestamp = int(time.mktime(time.strptime(date, '%Y-%m-%d %H:%M'))) 
		return timestamp

	def get_comment(self):
		print 'comment_list', self.comment_list
		for url in self.comment_list:
			self.driver.get(url)
			time.sleep(1)

			try:
				root_text = self.driver.find_element_by_xpath('//div[@id="m_story_permalink_view"]/div[1]/div/div[1]/div[2]').text
			except BaseException, e:
				root_text = ''
			print root_text
			
			try:
				root_mid = ''.join(re.search(re.compile('fbid%3D(\d+)%'),url).group(1))
				print 'root_mid', root_mid
			except BaseException, e:
				print "get_comment Position44444444", e 
				root_mid = ''
			print root_mid

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
					author_id = ''.join(re.findall(re.compile('id=(\d+)'),each.find_element_by_xpath('./div/h3/a').get_attribute('href')))
				except:
					author_id = ''					

				#try:
				#	print 7777777777
				#	pic_url = each.find_element_by_xpath('./div/div/div/div[1]/a/img').get_attribute('src')
				#except:
				#	pic_url = 'None'
				try:
					content = each.find_element_by_xpath('./div/div[1]').text
				except:
					try:
						content = each.find_element_by_xpath('./div/div[1]/span/span').text
					except:
						content = 'Emoji'
				print content

				try:
					ti = self.date2timestamp(str(each.text.replace(' ', '').split('·')[5]))
				except:
					try:
						ti = self.date2timestamp(str(each.text.replace(' ', '').split('·')[4]))
					except:
						ti = 0

				try:
					if re.findall(r'id=(\d+)&', each.find_element_by_xpath('./div/h3/a').get_attribute('href')):
						comment_type = 'receive'
						text = content
					else:
						comment_type = 'make'
						text = content
				except:
					comment_type = 'unknown'
					text = ''

				self.list.append({'uid':author_id, 'nick_name':author_name, 'mid':root_mid, 'timestamp':ti, 'text':content,'update_time':self.update_time, 'root_text':root_text, 'root_mid':root_mid, 'comment_type':comment_type})

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
