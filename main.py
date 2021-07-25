#!/usr/bin/env python

import requests
import json
from elasticsearch import Elasticsearch
# import os
# from pprint import pprint

github_api = "https://api.github.com"
github_session = requests.Session()
# Authenticated requests get a higher rate limit
# github_session.auth = (os.environ['GITHUB_USERNAME'], os.environ['GITHUB_TOKEN'])

# search ES and print pretty result
# def search(es_object, index_name, search):
#     res = es_object.search(index=index_name, body=search)
#     pprint(res)


def connect_elasticsearch():
    _es = None
    _es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    if _es.ping():
        print('ES Connected')
    else:
        print('Cannot connect to ES')
    return _es


def get_creation_date(username):
    url = github_api + '/users/{}'.format(username)
    commit_pg = github_session.get(url=url)
    commit_tp = json.loads(commit_pg.content)
    return commit_tp['created_at']


def create_index(es_object, index_name):
    created = False
    # index settings
    settings = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0
        },
        "mappings": {
            "commits": {
                "dynamic": "strict",
                "properties": {
                    "date": {
                        "type": "date"
                    },
                    "username": {
                        "type": "text"
                    },
                    "message": {
                        "type": "text"
                    },
                    "committer_account_creation_date": {
                        "type": "date"
                    },
                }
            }
        }
    }

    try:
        if not es_object.indices.exists(index_name):
            es_object.indices.create(
                index=index_name, ignore=400, body=settings)
            print('Created Index')
        created = True
    except Exception as ex:
        print(str(ex))
    finally:
        return created


def store_record(elastic_object, index_name, record):
    is_stored = True
    try:
        outcome = elastic_object.index(
            index=index_name, doc_type='commits', body=record)
        print(outcome)
    except Exception as ex:
        print('Error in indexing data')
        print(str(ex))
        is_stored = False
    finally:
        return is_stored


def get_metadata(commit):
    commit_date = commit['commit']['committer']['date']
    committer_username = commit['committer']['login']
    commit_message = commit['commit']['message']
    committer_account_creation_date = get_creation_date(committer_username)

    commit_meta = {
        'commit_date': commit_date,
        'committer_username': committer_username,
        'commit_message': commit_message,
        'committer_account_creation_date': committer_account_creation_date}
    return json.dumps(commit_meta)


if __name__ == '__main__':
    es = connect_elasticsearch()
    url = github_api + '/repos/rockstarlang/rockstar/commits'
    response = github_session.get(url=url)
    commits = json.loads(response.content)
    for commit in commits:
        commit_metadata = get_metadata(commit)
        print(commit_metadata)
        if es is not None:
            if create_index(es, 'metadata'):
                out = store_record(es, 'metadata', commit_metadata)
                if out:
                    print('Data indexed successfully')
                else:
                    print('Data index error')

    # search ES
    # if es is not None:
    #     search_object = {'query': {'match': {'committer_username': 'dylanbeattie'}}}
    #     search(es, 'metadata', json.dumps(search_object))
