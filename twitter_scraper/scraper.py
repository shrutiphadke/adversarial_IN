import requests
import os
import json
import pandas as pd
import time
import glob
from twitter_scraper import cred

#start_time=2020-05-25T17:00:00Z
#end_time=2021-12-07T01:00:00Z
def create_url(start_time,end_time,user_id,next_token):
    # Replace with user ID below
    if next_token==None:
        return "https://api.twitter.com/2/users/{}/tweets?max_results=100&start_time={}&end_time={}".format(user_id,start_time,end_time)
    else:
        return "https://api.twitter.com/2/users/{}/tweets?max_results=100&start_time={}&end_time={}&pagination_token={}".format(user_id,start_time,end_time,next_token)

    
def get_params(paramlist=None):
    # Tweet fields are adjustable.
    # Options include:
    # attachments, author_id, context_annotations,
    # conversation_id, created_at, entities, geo, id,
    # in_reply_to_user_id, lang, non_public_metrics, organic_metrics,
    # possibly_sensitive, promoted_metrics, public_metrics, referenced_tweets,
    # source, text, and withheld
    if paramlist:
        params = ",".join(paramlist)
    else:
        params = "created_at,entities,in_reply_to_user_id,lang,public_metrics,source,author_id"
    return {"tweet.fields": params}


def create_headers():
    bearer_token = cred.bearer_token
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers


def connect_to_endpoint(url, headers, params):
    response = requests.request("GET", url, headers=headers, params=params)
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(
            "Request returned an error: {} {}".format(
                response.status_code, response.text
            )
        )
    return response.json()

