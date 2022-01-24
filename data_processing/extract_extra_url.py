import numpy as np
import pandas as pd
import requests
import os
import json
import pandas as pd
import time
import glob

sample_data = pd.read_csv("/home/phadke/ONR/ONR/data_collection/sample_data_Jan_17.csv", header=0)

valid_files = []
for idx, row in sample_data.iterrows():
    cid = str(row['id_str'])
    screen_name=row['screen_name']
    party=row['party']
    state=row['state']
    
    fname = "/home/phadke/ONR/ONR/big_data/Twitter/" + cid+"_"+screen_name+"_"+party+".csv"
    valid_files.append(fname)

    
linkframe = pd.DataFrame(columns=['party','link', 'author'])
counter=0
for d in valid_files:
    if counter%100==0:
        print(counter)
    counter+=1
    try:
        splitfilename = d.replace(".csv", "").split("_")
        party = splitfilename[len(splitfilename)-1]
        author = splitfilename[len(splitfilename)-2]
        with open(d, "r") as jsonfile:

            for line in jsonfile:
                job= json.loads(line)
                if "entities" in job.keys():
                    if "urls" in job["entities"]:
                        for u in job["entities"]["urls"]:
                            if "expanded_url" in u:
                                linkframe = linkframe.append({"party":party, "link":u["expanded_url"], "author":author}, ignore_index=True)
                                
    except:
        fakevar = 0

        
linkframe.to_csv("/home/phadke/ONR/ONR/lite_data/jan21_nonfiltered_extracted_urls.csv")
## extract domains

def get_domain(link):
    try:
        res = get_tld(link, as_object=True)
        return res.fld
    except:
        fakevar=1


linkframe['domain'] = linkframe['link'].parallel_apply(lambda x: get_domain(x))

common_domains = ['twitter.com', 'facebook.com', 'google.com', 'm.tech', 'm.sc', 'b.tech', 'page.link', 'youtu.be', 'bit.ly', 'instagram.com','youtube.com']

filtered_link = linkframe.loc[~linkframe['domain'].isin(common_domains)]

filtered_link.to_csv("/home/phadke/ONR/ONR/lite_data/jan21_filtered_extracted_urls.csv")

agg_filtered = filtered_link.groupby(['author','domain']).size().reset_index().rename(columns={0:"count"})

print(agg_filtered.head())

agg_filtered.to_csv("/home/phadke/ONR/ONR/lite_data/jan21_extracted_domains.csv")