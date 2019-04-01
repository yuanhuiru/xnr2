# -*-coding:utf-8-*-
import sys
import json
from elasticsearch import Elasticsearch
sys.path.append('../../')
sys.path.append('/home/xnr1/xnr_0429/xnr')
sys.path.append('/home/xnr1/xnr_0429')
from global_utils import es_xnr as es, \
    info_monitor_index_name_pre, info_monitor_index_type


def info_monitor_mappings(date):
    index_name = info_monitor_index_name_pre + date
    index_info = {
        'settings':{
            'number_of_replicas':0,
            'number_of_shards':5,
            'analysis':{
                'analyzer':{
                    'my_analyzer':{
                    'type': 'pattern',
                    'pattern': '&'
                        }
                    }
                }
        },
        "mappings": {
            info_monitor_index_type: {
            "properties": {
              "platform": {                 # 社交平台 weibo || facebook || twitter
                "type": "string",
                "index": "not_analyzed"
              },
              "xnr_no": {                   # WXNR0001  ALL
                "type": "string",
                "index": "not_analyzed"
              },
              "type": {                     # 信息监测or用户检测
                "type": "string",
                "index": "not_analyzed"
              },
              "content": {                  # 帖子内容
                "type": "string",
                "index": "not_analyzed"
              },
            }
          }
        }
    }
    exist_indice=es.indices.exists(index=index_name)
    if not exist_indice:
        print es.indices.create(index=index_name,body=index_info,ignore=400)


def delete_mappings(date):
    index_name = info_monitor_index_name_pre + date
    print es.indices.delete(index=index_name, ignore=400)


if __name__=='__main__':
    #info_monitor_mappings('2019-03-31')
    delete_mappings('2019-04-01')



