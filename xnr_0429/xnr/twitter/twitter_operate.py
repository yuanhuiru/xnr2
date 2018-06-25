#!/usr/bin/env python
# encoding: utf-8

from selenium import webdriver
import requests
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import json
from lxml import etree
import re
from pybloom import BloomFilter
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import *
import tweepy
from tweepy import OAuthHandler
from elasticsearch import Elasticsearch
from launcher import Launcher
from Elasticsearch_tw import Es_twitter
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

class Operation():
	def __init__(self, username, password, consumer_key, consumer_secret, access_token, access_secret):
		self.launcher = Launcher(username, password, consumer_key, consumer_secret, access_token, access_secret)
		self.api = self.launcher.api()
		self.list = []

	def publish(self, text):
		try:
			self.api.update_status(text)
			time.sleep(2)
			print "publish Success!!!!!!"
			return [True, ' ']
		except Exception as e:
			print "publish Failed!!!!!!!"
			print e
			return [False, e]


	#API.update_with_media(filename[, status][, in_reply_to_status_id][, auto_populate_reply_metadata][, lat][, long][, source][, place_id][, file])
	def publish_with_media(self, filename, text):
		try:
			self.api.update_with_media(filename, text)
			time.sleep(2)
			print "publish_with_media Success!!!!!"
			return [True, ' ']
		except Exception as e:
			print "publish_with_media Failed!!!!!!"
			print e
			return [False, e]

	def mention(self, text):
		#text = '@lvleilei1 test'
		try:
			self.api.update_status(text)
			time.sleep(2)
			print "mention Success!!!!!!"
			return [True, ' ']
		except Exception as e:
			print "mention Failed!!!!!"
			print e
			return [False, e]

	def message(self, uid, text):
		try:
			print self.api.send_direct_message(uid, text=text)
			time.sleep(2)
			print "message Success!!!!!"
			return [True, ' ']
		except Exception as e:
			print "message Failed!!!!!"
			print e
			return [False, e]

	def follow(self, uid):
		try:
			self.api.create_friendship(uid)
			time.sleep(2)
			return [True, ' ']
		except Exception as e:
			return [False, e]

	def destroy_friendship(self, uid):
		try:
			self.api.destroy_friendship(uid)
			time.sleep(2)
			return [True, ' ']
		except Exception as e:
			return [False, e]

	def do_retweet(self, tid):
		try:
			self.api.retweet(tid)
			time.sleep(2)
			return [True, ' ']
		except Exception as e:
			return [False, e]


	def do_retweet_text(self, uid, tid, text):
		try:
			driver,display = self.launcher.login()
			screen_name = self.launcher.get_user(uid)
			post_url = 'https://twitter.com/' + screen_name + '/status/' + tid
			driver.get(post_url)
			time.sleep(3)
			current_url = driver.current_url
			pattern = re.compile('status/(\d+)')
			primary_id = ''.join(re.findall(pattern,current_url)).strip()
			driver.find_element_by_xpath('//button[@aria-describedby="profile-tweet-action-retweet-count-aria-%s"]/div'%primary_id).click()
			time.sleep(5)
			driver.find_element_by_xpath('//div[@id="retweet-with-comment"]').click()
			driver.find_element_by_xpath('//div[@id="retweet-with-comment"]').send_keys(text)
			driver.find_element_by_xpath('//button[@class="EdgeButton EdgeButton--primary retweet-action"]').click()
			return [True, ' ']
		except Exception as e:
			return [False, e]
		finally:
			driver.quit()
			display.popen.kill()

	def do_favourite(self, tid):
		try:
			self.api.create_favorite(tid)
			time.sleep(2)
			return [True, ' ']
		except Exception as e:
			return [False, e]

	def do_comment(self, uid, tid, text):
		try:
			driver,display = self.launcher.login()
			screen_name = self.launcher.get_user(uid)
			post_url = 'https://twitter.com/' + screen_name + '/status/' + tid
			driver.get(post_url)
			time.sleep(2)
			current_url = driver.current_url
			pattern = re.compile('status/(\d+)')
			primary_id = ''.join(re.findall(pattern,current_url)).strip()
			driver.find_element_by_xpath('//div[@id="tweet-box-reply-to-%s"]'%primary_id).send_keys(text)
			driver.find_element_by_xpath('//div[@id="tweet-box-reply-to-%s"]'%primary_id).send_keys(text)
			time.sleep(2)
			driver.find_element_by_xpath('//button[@class="tweet-action EdgeButton EdgeButton--primary js-tweet-btn"]').click()
			return [True, ' ']
		except Exception as e:
			return [False, e]
		finally:
			driver.quit()
			display.popen.kill()


if __name__ == '__main__':
	operation = Operation('8617078448226','xnr123456', 'N1Z4pYYHqwcy9JI0N8quoxIc1', 'VKzMcdUEq74K7nugSSuZBHMWt8dzQqSLNcmDmpGXGdkH6rt7j2', '943290911039029250-yWtATgV0BLE6E42PknyCH5lQLB7i4lr', 'KqNwtbK79hK95l4X37z9tIswNZSr6HKMSchEsPZ8eMxA9')
	#publish = operation.publish('06-18 test1')
	#print "发布成功!!!!!!!!!"
	#publish_with_media = operation.publish_with_media("../static/images/test.png", "twitter test")
	#print "发帖（带媒体文件）成功！"
	mention = operation.mention('@lvleilei1 05-10 test')
	print "mention成功！"
	message = operation.message('747226658457927680','05-10 test')
	print "message成功！"
	#follow = operation.follow('922372762651561984')
	#nofollow = operation.destroy_friendship('922372762651561984')
	#retweet = operation.do_retweet('984335883468922880')
	#retweet = operation.do_retweet_text('747226658457927680','923010915192016898','05-13 test')
	#favourite = operation.do_favourite('984335883468922880')
	#comment = operation.do_comment('747226658457927680','','05-1 test')
	#print(retweet)


