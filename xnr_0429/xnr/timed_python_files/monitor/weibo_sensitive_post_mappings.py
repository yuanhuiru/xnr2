# -*- coding:utf-8 -*-
import sys
import json
from global_utils import es_xnr as es,weibo_sensitive_post_index_name_pre,weibo_sensitive_post_index_type 


def weibo_sensitive_post_mappings(index_name):
    index_info = {
            'settings':{
                'analysis':{
                    'analyzer':{
                        'my_analyzer':{
                            'type': 'pattern',
                            'pattern': '&'
                        }
                    }
                }
            },
            'mappings':{
                weibo_sensitive_post_index_type:{
                    'properties':{
                        'xnr_user_no':{
                            'type':'string',
                            'index':'not_analyzed'
                        },
                        'search_type':{  #已关注用户 1，未关注用户-1
                             'type':'long'
                        },
                        'nick_name':{
                            'type':'string',
                            'index':'not_analyzed'
                        },
                        'uid':{
                            'type':'string',
                            'index':'not_analyzed'
                        },
                        'text':{
                            'type': 'string',
                            'index': 'not_analyzed'
                            },
                        'picture_url':{
                            'type':'string',
                            'index':'not_analyzed'
                        },
                        'vedio_url':{
                            'type':'string',
                            'index':'not_analyzed'
                        },
                        'user_fansnum':{
                            'type':'long'
                        },
                        'user_followersum':{
                            'type':'long'
                        },
                        'weibos_sum':{
                            'type':'long'
                        },
                        'mid':{
                            'type': 'string',
                            'index': 'not_analyzed'
                            },
                        'ip':{
                            'type': 'string',
                            'index': 'not_analyzed'
                            },
                        'directed_uid':{
                            'type':'long',
                            },
                        'directed_uname':{
                            'type': 'string',
                            'index': 'not_analyzed'
                            },
                        'timestamp':{
                            'type': 'long'
                            },
                        'sentiment': {
                            'type': 'string',
                            'index': 'not_analyzed'
                            },
                        'geo':{
                            'type': 'string',
                            'analyzer': 'my_analyzer'
                            },
                        'keywords_dict':{
                            'type': 'string',
                            'index': 'not_analyzed'
                            },
                        'keywords_string':{
                            'type': 'string',
                            'analyzer': 'my_analyzer'
                            },
                        'sensitive_words_dict':{
                            'type': 'string',
                            'index': 'not_analyzed'
                            },
                        'sensitive_words_string':{
                            'type': 'string',
                            'analyzer': 'my_analyzer'
                            },
                        'message_type':{
                            'type': 'long'
                            },
                        'root_uid':{
                            'type': 'string',
                            'index': 'not_analyzed'
                            },
                        'root_mid':{
                            'type': 'string',
                            'index': 'not_analyzed'
                            },
                         # uncut weibo text
                        'origin_text':{
                            'type': 'string',
                            'index': 'not_analyzed'
                            },
                        'origin_keywords_dict':{
                            'type': 'string',
                            'index': 'not_analyzed'
                            },
                        'origin_keywords_string':{
                            'type': 'string',
                            'analyzer': 'my_analyzer'
                            },
                        'comment':{
                            'type':'long'
                            },
                        'sensitive':{
                            'type':'long'
                            },
                        'retweeted':{
                            'type':'long'
                            }
                        }
                    }
                }
            }
    exist_indice = es.indices.exists(index=index_name)
    if not exist_indice:
        es.indices.create(index=index_name, body=index_info, ignore=400)
        print index_name

if __name__=='__main__':
    index_name = weibo_sensitive_post_index_name_pre + '2018-06-17'
    weibo_sensitive_post_mappings(index_name)
