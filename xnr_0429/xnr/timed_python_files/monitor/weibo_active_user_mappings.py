# -*-coding:utf-8-*-
import os
import json
import time
from elasticsearch import Elasticsearch
import sys
sys.path.append('../../')
from global_utils import es_xnr as es,weibo_active_user_index_name_pre,weibo_active_user_index_type
from time_utils import ts2datetime

def weibo_active_user_mappings(index_name):
	index_info = {
		'settings':{
			'number_of_replicas':0,
			'number_of_shards':5
		},
		'mappings':{
			weibo_active_user_index_type:{
				'properties':{
					'uid':{
						'type':'string',
						'index':'not_analyzed'
					},
					'url':{
						'type':'string',
						'index':'not_analyzed'
					},
					'uname':{
						'type':'string',
						'index':'not_analyzed'
					},
					'location':{
						'type':'string',
						'index':'not_analyzed'
					},
					'fans_num':{
						'type':'long'
					},
					'total_number':{
						'type':'long'
					},	
					'influence':{
						'type':'long'
					},				
					'timestamp':{
						'type':'long'
					}
				}
			}
		}
	}

	if not es.indices.exists(index=index_name):
		es.indices.create(index=index_name,body=index_info,ignore=400)




if __name__ == '__main__':
    index_name = weibo_active_user_index_name_pre + ts2datetime(int(time.time())) 
    weibo_active_user_mappings(index_name)




