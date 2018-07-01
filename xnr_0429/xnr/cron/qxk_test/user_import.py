import os
import json
import time
import sys
reload(sys)
sys.path.append('../../')
from global_utils import es_xnr,es_user_portrait,es_user_profile
from global_utils import weibo_date_remind_index_name,weibo_date_remind_index_name_test,weibo_date_remind_index_type,\
                         weibo_hidden_expression_index_name,weibo_hidden_expression_index_name_test,weibo_hidden_expression_index_type

weibo_active_user_index_name_pre = 'weibo_active_user_2018-06-27'
weibo_active_user_index_type = 'user'
profile_index_name = 'weibo_user'  # user profile es
profile_index_type = 'user'
weibo_bci_index_name_pre = 'bci_20180627'
weibo_bci_index_type = 'bci'

portrait_index_name = 'user_portrait_1222'
portrait_index_type = 'user'


def export_date():
    query_body={
        'query':{
            'match_all':{}
        },
        'size':1000,
        'sort':{'influence':{'order':'desc'}}
    }
    result=es_user_portrait.search(index=portrait_index_name, doc_type=portrait_index_type, body=query_body)['hits']['hits']
    id_list = [user['_id'] for user in result]
    print len(id_list) 
    final_results = []
    for idx, uid in enumerate(id_list):
        print idx, 'over!!'
        try:
            user_bci = es_user_portrait.get(index=portrait_index_name, doc_type=portrait_index_type, id=uid)['_source']
            user_profile = es_user_profile.get(index=profile_index_name, doc_type=profile_index_type, id=uid)['_source']
            hb = dict(user_bci.items() + user_profile.items())
            final_results.append(hb)
        except:
            print 'not found', uid
    print 'final len', len(final_results)
    fw = file('high_influence_user.json', 'w')
    fw.write(json.dumps(final_results))
    fw.close()
    # return results

def export_random_user():
    import random
    query_body={
        'query':{
            'match_all':{}
        },
        'size':50000
    }
    result=es_user_portrait.search(index=portrait_index_name, doc_type=portrait_index_type, body=query_body)['hits']['hits']
    id_list = [user['_id'] for user in result]
    random.shuffle(id_list)
    print type(id_list), len(id_list)
    id_list = id_list[:9000]
    
    print len(id_list)
    final_results = []
    for idx, uid in enumerate(id_list):
        try:
            user_bci = es_user_portrait.get(index=portrait_index_name, doc_type=portrait_index_type, id=uid)['_source']
            user_profile = es_user_profile.get(index=profile_index_name, doc_type=profile_index_type, id=uid)['_source']
            hb = dict(user_bci.items() + user_profile.items())
            final_results.append(hb)
            print idx, 'over!!'
        except:
            print 'not found', uid
    print 'final len', len(final_results)

    fw = file('random_user.json', 'w')
    fw.write(json.dumps(final_results))
    fw.close()
    # return results

if __name__ == '__main__':
    export_date()
    export_random_user()
