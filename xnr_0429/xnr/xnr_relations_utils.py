#-*-coding:utf-8-*-
import json
import time
from global_utils import es_xnr, es_user_portrait,\
    weibo_xnr_relations_index_name, weibo_xnr_relations_index_type,\
    portrait_index_name, portrait_index_type
from utils import uid2xnr_user_no
from
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

def load_pingtaiguanzhu_state(root_uid, uid):
    """
    :param root_uid:
    :param uid:
    :return: 现在表中记录的xnr和user之间的平台关注关系
    """
    pingtaiguanzhu_state = 0
    query_body = {
        'query':{
            'filtered':{
                'filter':{
                    'bool':{
                        'must':[
                            {'term': {'xnr_uid': root_uid}},
                            {'term': {'uid': uid}}
                        ]
                    }
                }
            }
        }
    }
    search_results = es_xnr.search(index=weibo_xnr_relations_index_name, doc_type=weibo_xnr_relations_index_type, body=query_body)['hits']['hits']
    if search_results:
        pingtaiguanzhu_state = int(search_results[0]['_source']['pingtaiguanzhu'])
    return pingtaiguanzhu_state

def update_weibo_xnr_relations(root_uid, uid, data, update_portrait_info=False):
    # pingtaiguanzhu 决定了是否要在平台上真正关注该用户，涉及到更改该关系时，一定要指定该字段（1或0）。
    '''
    :param root_uid: xnr_uid
    :param uid: user_uid
    :param data: relation data. eg: data={'gensuiguanzhu': 1, 'pingtaiguanzhu': 1}
    :param update_portrait_info: update or not
    :return: update success or not
    '''
    xnr_user_no = uid2xnr_user_no(root_uid)
    if xnr_user_no:
        data['platform'] = 'weibo'
        data['xnr_no'] = xnr_user_no
        data['xnr_uid'] = root_uid
        data['uid'] = uid

        '''
        pingtaiguanzhu = data.get('pingtaiguanzhu', -1)
        pingtaiguanzhu_state  = load_pingtaiguanzhu_state(root_uid, uid)
        if pingtaiguanzhu != pingtaiguanzhu_state:
            if pingtaiguanzhu == 1:
                'gaunzhu'
                data['pingtaiguanzhu'] = 1
            elif pingtaiguanzhu == 0:
                'quxiao'
                data['pingtaiguanzhu'] = 0
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
            return True
        except Exception,e:
            print 'update_weibo_xnr_relations Error: ', str(e)
    return False


