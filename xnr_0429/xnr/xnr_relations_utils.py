#-*-coding:utf-8-*-


# weibo
from global_utils import es_xnr, es_user_portrait,\
    weibo_xnr_relations_index_name, weibo_xnr_relations_index_type,\
    portrait_index_name, portrait_index_type
from sina.weibo_operate import SinaOperateAPI
from global_utils import es_xnr,weibo_xnr_index_name,weibo_xnr_index_type
from utils import uid2xnr_user_no


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


def load_user_passwd(uid):
    try:
        query_body = {
        'query':{
            'term':{'uid':uid}
        }
    }
        result = es_xnr.search(index=weibo_xnr_index_name,doc_type=weibo_xnr_index_type,body=query_body)['hits']['hits']
        print result
        #print result
        xnr_user_no = result[0]['_source']['xnr_user_no']
        weibo_password = result[0]['_source']['password']
        weibo_account = result[0]['_source']['weibo_phone_account']
        if weibo_account == '':
            weibo_account = result[0]['_source']['weibo_mail_account']

    except:
        xnr_user_no = ''
        weibo_password = ''
        weibo_account = ''

    return weibo_account, weibo_password



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

        # 调用爬虫 平台关注 2019年5月31日
        pingtaiguanzhu = data.get('pingtaiguanzhu', -1)
        pingtaiguanzhu_state  = load_pingtaiguanzhu_state(root_uid, uid)
        account, password = load_user_passwd(root_uid)
        print(account, password)
        sina_operate_api = SinaOperateAPI(account, password)
        if pingtaiguanzhu != pingtaiguanzhu_state:
            if pingtaiguanzhu == 1:
                print 'go to follow--------------------------------------'
                print sina_operate_api.followed(uid=uid)
                #'gaunzhu'
                data['pingtaiguanzhu'] = 1
            elif pingtaiguanzhu == 0:
                #'quxiao'
                data['pingtaiguanzhu'] = 0

        try:
            print "one-------------"
            _id = '%s_%s' % (root_uid, uid)
            user_exist =  es_xnr.exists(index=weibo_xnr_relations_index_name, doc_type=weibo_xnr_relations_index_type, id=_id)
            if user_exist:
                print "two-------------"
                if update_portrait_info:
                    protrait_info = update_weibo_user_portrait_info(uid)
                    data.update(protrait_info)
                es_result = es_xnr.update(index=weibo_xnr_relations_index_name, doc_type=weibo_xnr_relations_index_type, id=_id, body={'doc': data})
            else:
                print "three-------------"
                protrait_info = update_weibo_user_portrait_info(uid)
                data.update(protrait_info)
                es_result = es_xnr.index(index=weibo_xnr_relations_index_name, doc_type=weibo_xnr_relations_index_type, id=_id, body=data)
            return True
        except Exception,e:
            print 'update_weibo_xnr_relations Error: ', str(e)
    return False



# facebook
from global_utils import es_xnr_2, es_fb_user_portrait, \
    facebook_xnr_relations_index_name, facebook_xnr_relations_index_type, \
    fb_portrait_index_name, fb_portrait_index_type
from utils import fb_uid2xnr_user_no


def update_facebook_user_portrait_info(uid):
    user_exist = es_fb_user_portrait.exists(index=fb_portrait_index_name, doc_type=fb_portrait_index_type, id=uid)
    if user_exist:
        user_data= es_fb_user_portrait.get(index=fb_portrait_index_name,doc_type=fb_portrait_index_type,id=uid)['_source']
        portrait_info = {
            'influence': user_data.get('influence', 0),
            'sensitive': user_data.get('sensitive', 0),
            'topic_string': user_data.get('topic_string',''),
        }
        return portrait_info
    return {'influence': 0, 'sensitive': 0, 'topic_string': ''}


def load_facebook_pingtaiguanzhu_state(root_uid, uid):
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
    search_results = es_xnr_2.search(index=facebook_xnr_relations_index_name, doc_type=facebook_xnr_relations_index_type, body=query_body)['hits']['hits']
    if search_results:
        pingtaiguanzhu_state = int(search_results[0]['_source']['pingtaiguanzhu'])
    return pingtaiguanzhu_state


def load_facebook_relation_uids(xnr_user_no, term_query_list):
    """

    :param xnr_user_no:
    :param term_query_list: term语句列表
    :return: 根据term语句，返回搜到的跟xnr有关系的人的uid列表
    """
    uids = []
    query_body = {
        'query':{
            'filtered':{
                'filter':{
                    'bool':{
                        'must':[
                            {'term': {'xnr_no': xnr_user_no}},
                        ]
                    }
                }
            }
        }
    }
    query_body['query']['filtered']['filter']['bool']['must'].extend(term_query_list)
    search_results = es_xnr_2.search(index=facebook_xnr_relations_index_name, doc_type=facebook_xnr_relations_index_type, body=query_body)['hits']['hits']
    for search_result in search_results:
        uid = search_result['_source']['uid']
        uids.append(uid)
    return uids


def load_facebook_relation_num(xnr_user_no, term_query_list):
    """

    :param xnr_user_no:
    :param term_query_list: term语句列表
    :return: 根据term语句，返回搜到的跟xnr有关系的人的数目
    """
    return len(load_facebook_relation_uids(xnr_user_no, term_query_list))


def update_facebook_xnr_relations(root_uid, uid, data, update_portrait_info=False):
    # pingtaiguanzhu 决定了是否要在平台上真正关注该用户，涉及到更改该关系时，一定要指定该字段（1或0）。
    '''
    :param root_uid: xnr_uid
    :param uid: user_uid
    :param data: relation data. eg: data={'gensuiguanzhu': 1, 'pingtaiguanzhu': 1}
    :param update_portrait_info: update or not
    :return: update success or not
    '''
    xnr_user_no = fb_uid2xnr_user_no(root_uid)
    if xnr_user_no:
        data['platform'] = 'facebook'
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
            user_exist =  es_xnr_2.exists(index=facebook_xnr_relations_index_name, doc_type=facebook_xnr_relations_index_type, id=_id)
            if user_exist:
                if update_portrait_info:
                    protrait_info = update_facebook_user_portrait_info(uid)
                    data.update(protrait_info)
                es_result = es_xnr_2.update(index=facebook_xnr_relations_index_name, doc_type=facebook_xnr_relations_index_type, id=_id, body={'doc': data})
            else:
                protrait_info = update_facebook_user_portrait_info(uid)
                data.update(protrait_info)
                es_result = es_xnr_2.index(index=facebook_xnr_relations_index_name, doc_type=facebook_xnr_relations_index_type, id=_id, body=data)
            print es_result
            return True
        except Exception,e:
            print 'update_facebook_xnr_relations Error: ', str(e)
    return False




# twitter
from global_utils import es_xnr_2, es_tw_user_portrait, \
    twitter_xnr_relations_index_name, twitter_xnr_relations_index_type, \
    tw_portrait_index_name, tw_portrait_index_type
from utils import tw_uid2xnr_user_no


def update_twitter_user_portrait_info(uid):
    user_exist = es_tw_user_portrait.exists(index=tw_portrait_index_name, doc_type=tw_portrait_index_type, id=uid)
    if user_exist:
        user_data= es_tw_user_portrait.get(index=tw_portrait_index_name,doc_type=tw_portrait_index_type,id=uid)['_source']
        portrait_info = {
            'influence': user_data.get('influence', 0),
            'sensitive': user_data.get('sensitive', 0),
            'topic_string': user_data.get('topic_string',''),
        }
        return portrait_info
    return {'influence': 0, 'sensitive': 0, 'topic_string': ''}


def load_twitter_pingtaiguanzhu_state(root_uid, uid):
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
    search_results = es_xnr_2.search(index=twitter_xnr_relations_index_name, doc_type=twitter_xnr_relations_index_type, body=query_body)['hits']['hits']
    if search_results:
        pingtaiguanzhu_state = int(search_results[0]['_source']['pingtaiguanzhu'])
    return pingtaiguanzhu_state


def update_twitter_xnr_relations(root_uid, uid, data, update_portrait_info=False):
    # pingtaiguanzhu 决定了是否要在平台上真正关注该用户，涉及到更改该关系时，一定要指定该字段（1或0）。
    '''
    :param root_uid: xnr_uid
    :param uid: user_uid
    :param data: relation data. eg: data={'gensuiguanzhu': 1, 'pingtaiguanzhu': 1}
    :param update_portrait_info: update or not
    :return: update success or not
    '''
    xnr_user_no = tw_uid2xnr_user_no(root_uid)
    if xnr_user_no:
        data['platform'] = 'twitter'
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
            user_exist =  es_xnr_2.exists(index=twitter_xnr_relations_index_name, doc_type=twitter_xnr_relations_index_type, id=_id)
            if user_exist:
                if update_portrait_info:
                    protrait_info = update_twitter_user_portrait_info(uid)
                    data.update(protrait_info)
                es_result = es_xnr_2.update(index=twitter_xnr_relations_index_name, doc_type=twitter_xnr_relations_index_type, id=_id, body={'doc': data})
            else:
                protrait_info = update_twitter_user_portrait_info(uid)
                data.update(protrait_info)
                es_result = es_xnr_2.index(index=twitter_xnr_relations_index_name, doc_type=twitter_xnr_relations_index_type, id=_id, body=data)
            return True
        except Exception,e:
            print 'update_twitter_xnr_relations Error: ', str(e)
    return False



if __name__ == '__main__':
    account, password = load_user_passwd('5762691364')
    print account, password
