#!/usr/bin/env python
#encoding: utf-8

from launcher import Launcher
from Elasticsearch_tw import Es_twitter
import datetime
import time

class Message():
	def __init__(self,username, password, consumer_key, consumer_secret, access_token, access_secret):
		self.launcher = Launcher(username, password, consumer_key, consumer_secret, access_token, access_secret)
		self.api = self.launcher.api()
		self.es = Es_twitter()
		self.list = []
		self.root_uid = self.api.me()
		self.update_time = int(time.time())

	def get_message(self):
		for each in self.api.direct_messages():
			content = each.text
			sender_screen_name = each.sender._json['screen_name']
			sender_user_name = each.sender._json['name']
			sender_id = each.sender._json['id']
			timestamp = int(time.mktime(each.created_at.timetuple()))
			photo_url = each.sender._json['profile_image_url_https']
			if sender_id == each.recipient_id_str:
				private_type = 'make'
				root_text = content
				text = ''
			else:
				private_type = 'receive'
				root_text = ''
				text = content
			item = {
				'uid':sender_id,
				'photo_url':photo_url,
				'user_name':sender_screen_name,
				'nick_name':sender_user_name,
				'timestamp':timestamp,
				'text':text,
				'root_text':root_text,
				'private_type':private_type,
				'update_time':self.update_time
			}
			self.list.append(item)
		return self.list

	def save(self,indexName,typeName,message_list):
		self.es.executeES(indexName,typeName,message_list)

if __name__ == '__main__':
	message = Message('8617078448226', 'xnr123456', 'N1Z4pYYHqwcy9JI0N8quoxIc1', 'VKzMcdUEq74K7nugSSuZBHMWt8dzQqSLNcmDmpGXGdkH6rt7j2', '943290911039029250-yWtATgV0BLE6E42PknyCH5lQLB7i4lr', 'KqNwtbK79hK95l4X37z9tIswNZSr6HKMSchEsPZ8eMxA9')
	message_list = message.get_message()
	print message_list
	#message.save('twitter_feedback_private','text',message_list)
