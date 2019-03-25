# -*-coding:utf-8-*-
import sys
import json
from elasticsearch import Elasticsearch
from global_utils import es_xnr_2 as es,\
    facebook_xnr_relations_index_name, facebook_xnr_relations_index_type

def facebook_xnr_relations_mappings():
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
          facebook_xnr_relations_index_type: {
            "properties": {
              "platform": {                 # 社交平台 weibo || facebook || twitter
                "type": "string",
                "index": "not_analyzed"
              },
              "xnr_no": {                   # FXNR0001
                "type": "string",
                "index": "not_analyzed"
              },
              "xnr_uid": {                  # xnr的uid
                "type": "string",
                "index": "not_analyzed"
              },
              "uid": {                      # 用户的uid
                "type": "string",
                "index": "not_analyzed"
              },
              "nickname": {
                "type": "string",
                "index": "not_analyzed"
              },
              "sex": {                      # male female unknown
                "type": "integer",
              },
              "richangguanzhu": {           # 1=是，0=否
                "type": "integer",
              },
              "yewuguanzhu": {              # 1=是，0=否
                "type": "integer",
              },
              "gensuiguanzhu": {            # 1=是，0=否
                "type": "integer",
              },
              "pingtaihaoyou": {           # 1=是，0=否 。 xnr在社交平台上关注的用户
                "type": "integer",
              },
              "influence": {
                "type": "double"
              },
              "photo_url": {
                "type": "string",
                "index": "not_analyzed"
              },
              "remark": {
                "type": "string",
                "index": "not_analyzed"
              },
              "sensitive": {
                "type": "double"
              },
              "topic_string": {
                "type": "string",
                "analyzer": "my_analyzer"
              },
              "haoyou_num": {                # 用户的好友数
                "type": "integer",
              },
              "geo": {                      # 位置
                "type": "string",
                "index": "not_analyzed"
              }
            }
          }
        }
    }
    exist_indice=es.indices.exists(index=facebook_xnr_relations_index_name)
    if not exist_indice:
        print es.indices.create(index=facebook_xnr_relations_index_name,body=index_info,ignore=400)

def delete_mappings():
    print es.indices.delete(index=facebook_xnr_relations_index_name, ignore=400)


"""
def update_mappings():
    mappings = {
            'text':{
                'properties':{                    
                    'date_time':{                #日期，例如：2017-09-07
                        'type':'string',
                        'index':'not_analyzed'
                    },
                    'keyword_value_string':{                #关键词统计结果
                        'type':'string',
                        'index':'no'
                    },
                    'timestamp':{ # 时间戳
                        'type':'long'
                    },
                    'test':{ # 时间戳
                        'type':'long'
                    },
                }
            }
        }
    print es.indices.put_mapping(index='test_mappings', doc_type='text', body=mappings)
"""

if __name__=='__main__':
    facebook_xnr_relations_mappings()
    # delete_mappings()



