#-*- coding: utf-8 -*-
import sys
import json
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
from global_utils import es_xnr as es
from global_utils import network_buzzwords_index_name, network_buzzwords_index_type

def network_buzzwords_mapping():
	index_info = {
		'settings':{
			'number_of_replicas':0,
			'number_of_shards':5
		},
		'mappings':{
			network_buzzwords_index_type:{
				'properties':{
					'uid':{         #操作虚拟人的uid
						'type':'string',
						'index':'not_analyzed'
					},
					'text_list':{    #照片url
						'type':'string',
						'index':'not_analyzed'
					},
					'type':{       #点赞对象昵称
						'type':'string',
						'index':'not_analyzed'
					},
				}
			}
		}
	}

	if not es.indices.exists(index=network_buzzwords_index_name):
		es.indices.create(index=network_buzzwords_index_name,body=index_info,ignore=400)

if __name__ == '__main__':
    network_buzzwords_mapping()


