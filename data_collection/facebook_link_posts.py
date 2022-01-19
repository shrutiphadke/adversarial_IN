import pandas as pd
import json
from collections import defaultdict
from collections import Counter
import time 
from six.moves import urllib
import wget
import logging
logging.basicConfig(filename='facebook_lp_jan19.log',level=logging.DEBUG)
import pickle as pkl
import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse, parse_qs
import glob

month_conv = {1:'01', 2:'02', 3:'03', 4:'04', 5:'05', 6:'06', 7:'07', 8:'08', 9:'09', 10:'10', 11:'11', 12:'12'}
higher_date = {1:31, 2:28, 3:31, 4:30, 5:31, 6:30, 7:31, 8:31, 9:30, 10:31, 11:30, 12:31}

domains = pd.read_csv("/home/phadke/ONR/ONR/lite_data/domains_for_facebook.csv", header=0)

done_fnames = [x.replace('/data/shruti/ONR/big_data/facebook_linkposts/', "") for x in glob.glob("/data/shruti/ONR/big_data/facebook_linkposts/*")]

domains = domains.loc[~domains['domain'].isin(done_fnames)]

links = domains.loc[domains['is_news']==1]['domain'].tolist()

print(links)

for d in links:
    folderpath = "/data/shruti/ONR/big_data/facebook_linkposts/" + d + "/"
    if not os.path.exists(folderpath):
                os.makedirs(folderpath)


    for m in range(1, 13):
        month = "{0:0=2d}".format(m)
        newfile = folderpath + "2021_" + month + ".json"
        with open(newfile, "w") as jfile:
            for dt in range(1, higher_date[m]+1):
                lower_date = "2021-" + month + "-"+"{0:0=2d}".format(dt)
                upper_date = "2021-" + month + "-"+"{0:0=2d}".format(dt+1)
                q = 'https://api.crowdtangle.com/links?token=IOspkA7AKgiNqZPvmhGDJ7xxSJWYYErUtZCUiBOh&link=' + d + '&startDate=' + lower_date + '&endDate=' + upper_date + '&platforms=facebook&sortBy=total_interactions&count=1000'
                
                
                try:
                    response = requests.get(q)
                    q_respose = json.loads(response.content.decode('utf-8'))
                    postcontainer  = q_respose['result']['posts']
                except:
                    logstr = d + "_" + lower_date + "_" + upper_date + ":fail"
                    logging.info(logstr)
                    postcontainer = []

                if len(postcontainer) > 0:
                    for p in postcontainer:
                        json.dump(p, jfile)
                        #jfile.write(p)
                        jfile.write("\n")
                        
                        
