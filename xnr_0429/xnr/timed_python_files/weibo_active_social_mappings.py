#-*-coding:utf-8-*-

import sys
import time
import json
sys.path.append('../')
from global_utils import es_xnr as es, active_social_index_name_pre, active_social_index_type
from time_utils import datetime2ts,ts2datetime

def active_social_mappings(index_name):

	index_info = {
        'settings':{
            'number_of_replicas':0,
            'number_of_shards':5
        },
		
		'mappings':{
            active_social_index_type:{
                'properties':{
                    'xnr_user_no':{
                        'type':'string',
                        'index':'not_analyzed'
                    },
					'sort_item':{
						'type':'string',
						'index':'not_analyzed'
					},
					'result':{
						'type':'string',
						'index':'no'
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

	current_time = time.time() #+ 24*3600
	current_date = ts2datetime(current_time)    
	
	index_name = active_social_index_name_pre + current_date 
	print 'index_name:',index_name
	active_social_mappings(index_name)	
