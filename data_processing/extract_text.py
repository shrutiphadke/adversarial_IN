import pandas as pd
import numpy as np
from collections import defaultdict, Counter
import glob
import json
from random import sample
import sklearn
import re
import string
import warnings
from bs4 import BeautifulSoup
import gensim
from gensim.models.phrases import Phrases
import gensim.corpora as corpora
from gensim.utils import simple_preprocess
from gensim.models import CoherenceModel
from analysis_func.text_preproc import preproc_text
from analysis_func.topic_preproc import prepare_corp
from analysis_func.topic_model import topicmodel
from analysis_func.saveload_topicmodels import save_modelanddata, load_modelanddata
import datetime
import os
import time
import sys


from pandarallel import pandarallel
pandarallel.initialize()

print("getting data")
# get random samples from each party of similar sizes
datafiles = glob.glob("/home/phadke/ONR/ONR/big_data/Twitter/*.csv")
# plist = ['_BJP.csv', '_INC.csv', '_AAP.csv']
# pcount = defaultdict(list)
# for d in datafiles:
#     for p in plist:
#         if p in d:
#             pcount[p].append(d)
                
# final_files = defaultdict(list)
# for p in pcount:
#     try:
#         final_files[p] = sample(pcount[p], 1000)
#     except:
#         final_files[p] = sample(pcount[p], 430)
    
    
#read randomly sampled data into a dataframe
counter=0
rowlist = []
for d in datafiles:
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
                if "text" in job:
                    text = job['text']
                else:
                    text = None
                if "screen_name" in job:
                    sn = job['screen_name']
                else:
                    sn = None
                    
                if "lang" in job:
                    language = job['lang']
                else:
                    language = None
                    
                url = ""
                if "entities" in job:
                    if "urls" in job["entities"]:
                        for u in job["entities"]["urls"]:
                            if "expanded_url" in u:
                                url = u["expanded_url"]

                    
                row = [sn, text, party, language, url]
                rowlist.append(row)
                
    except:
        fakevar=0

                
frame = pd.DataFrame(rowlist, columns=['screen_name','text','party', 'language', 'url'])
frame = frame.dropna(subset=['text'])
frame.to_csv("/data/shruti/ONR/small_data/Twitter_text/twitter_text_bjp_inc.csv")

def startwithRT(text):
    if text.startswith("RT "):
        return 1
    
frame['is_RT'] = frame['text'].parallel_apply(lambda x: startwithRT(x))
frame.to_csv("/data/shruti/ONR/small_data/Twitter_text/twitter_text_bjp_inc_withRT.csv")