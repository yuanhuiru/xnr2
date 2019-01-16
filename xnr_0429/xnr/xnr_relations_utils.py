#-*-coding:utf-8-*-
import json
import time
from global_utils import es_xnr, es_user_portrait,\
    weibo_xnr_relations_index_name, weibo_xnr_relations_index_type,\
    portrait_index_name, portrait_index_type

def update_weibo_user_portrait_info(uid):
    user_exist = es_user_portrait.exists(index=portrait_index_name, doc_type=portrait_index_type, id=uid)
    if user_exist:
        user_data= es_user_portrait.get(index=portrait_index_name,doc_type=portrait_index_type,id=uid)['_source']
        portrait_info = {
            'influence': user_data.get('influence', 0),
            'sensitive': user_data.get('sensitive', 0),
            'topic_string': user_data.get('topic_string',''),
        }
        return portrait_info
    return {'influence': 0, 'sensitive': 0, 'topic_string': ''}


def update_weibo_xnr_relations(root_uid, uid, data, update_portrait_info=False):
    '''
    :param root_uid: xnr_uid
    :param uid: user_uid
    :param data: relation data
    :param update_portrait_info: update or not
    :return: update success or not
    '''
    try:
        _id = '%s_%s' % (root_uid, uid)
        user_exist =  es_xnr.exists(index=weibo_xnr_relations_index_name, doc_type=weibo_xnr_relations_index_type, id=_id)
        if user_exist:
            if update_portrait_info:
                protrait_info = update_weibo_user_portrait_info(uid)
                data.update(protrait_info)
            es_result = es_xnr.update(index=weibo_xnr_relations_index_name, doc_type=weibo_xnr_relations_index_type, id=_id, body={'doc': data})
        else:
            protrait_info = update_weibo_user_portrait_info(uid)
            data.update(protrait_info)
            es_result = es_xnr.index(index=weibo_xnr_relations_index_name, doc_type=weibo_xnr_relations_index_type, id=_id, body=data)
    except Exception,e:
        print 'update_weibo_xnr_relations Error: ', str(e)
