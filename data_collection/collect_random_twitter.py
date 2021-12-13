import numpy as np
import pandas as pd
import requests
import os
import json
import pandas as pd
import time
import glob

from twitter_scraper.scraper import create_url, get_params, create_headers, connect_to_endpoint
idlist = ["1346963316"]


include_parties = ['BJP', 'INC']

data = pd.read_csv("/home/phadke/ONR/ONR/lite_data/india_July_21.csv", header=0)

add_on = True
sample_size = 5000

if add_on:
    completed_collection = pd.read_csv("/home/phadke/ONR/ONR/data_collection/sample_data_dec_7.csv", header=0)
    completed_screens = completed_collection['screen_name'].tolist()
    data = data.loc[~data['screen_name'].isin(completed_screens)]


sample_data_bjp = data.loc[data['party']=='BJP'].sample(sample_size)
sample_data_inc = data.loc[data['party']=='INC'].sample(sample_size)

sample_data = pd.concat([sample_data_bjp,sample_data_inc])

sample_data.to_csv("/home/phadke/ONR/ONR/data_collection/sample_data_dec_13.csv")

for idx, row in sample_data.iterrows():
    time.sleep(0.6)
    cid = str(row['id_str'])
    screen_name=row['screen_name']
    party=row['party']
    state=row['state']
    
    fname = "/home/phadke/ONR/ONR/big_data/Twitter/" + cid+"_"+screen_name+"_"+party+".csv"
    
    
    start_time="2020-05-01T01:00:00Z"
    end_time="2021-12-07T01:00:00Z"
    
    url = create_url(start_time,end_time,user_id = cid, next_token=None)
    
    print(url)
    headers = create_headers()
    params = get_params()

    json_response = connect_to_endpoint(url, headers, params)
    time.sleep(1)
    if "data" in json_response and len(json_response['data'])>0: 
        with open(fname, "w") as tfile:
            for j in json_response['data']:
                json.dump(j, tfile)
                tfile.write("\n")

            while  "meta" in json_response and "next_token" in json_response['meta']:
                url = create_url(start_time,end_time,user_id = cid, next_token=json_response['meta']['next_token'])
                json_response = connect_to_endpoint(url, headers, params)
                time.sleep(1)
                if "data" in json_response and len(json_response['data']) >0:
                    for j in json_response['data']:
                        j['screen_name']=screen_name
                        j['party']=party
                        j['state']=state
                        json.dump(j, tfile)
                        tfile.write("\n")

