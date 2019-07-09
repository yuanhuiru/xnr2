from elasticsearch import Elasticsearch


ES_CLUSTER_HOST = ['192.168.169.45:9205','192.168.169.47:9205','192.168.169.47:9206']
es = Elasticsearch(ES_CLUSTER_HOST, timeout=600)


weibo_xnr_relations_index_name = 'weibo_xnr_relations'
weibo_xnr_relations_index_type = 'user'

def get_show_trace_followers(xnr_user_no):
    results = []
    query_body = {
        'query':{
            'filtered':{
                'filter':{
                    'bool':{
                        'must':[
                            {'term': {'xnr_no': xnr_user_no}},
                            {'term': {'gensuiguanzhu': 1}}
                        ]
                    }
                }
            }
        },
        'size': 999,
    }
    search_results = es.search(index=weibo_xnr_relations_index_name, doc_type=weibo_xnr_relations_index_type, body=query_body)['hits']['hits']
    print search_results
    for data in search_results:
        data = data['_source']
        r = {
            'uid': data.get('uid', ''),
            'nick_name': data.get('nickname', ''),
            'fansnum': data.get('fensi_num', 0),
            'follownum': data.get('guanzhu_num', 0),
            'sex': data.get('sex', 'unknown'),
            'photo_url': data.get('photo_url', ''),
            'statusnum': 0,
            'user_location': data.get('geo', ''),
        }
        results.append(r)
    return results


if __name__ == "__main__":
    results = get_show_trace_followers('WXNR0065') 
