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
import pandas as pd
import numpy as np
from collections import defaultdict, Counter

from pandarallel import pandarallel
pandarallel.initialize()


print("getting data")
# get random samples from each party of similar sizes
datafiles = glob.glob("/data/shruti/ONR/big_data/Twitter/*.csv")
plist = ['_BJP.csv', '_INC.csv', '_AAP.csv']
pcount = defaultdict(list)
for d in datafiles:
    for p in plist:
        if p in d:
            pcount[p].append(d)
                
final_files = pcount
#final_files = defaultdict(list)
# for p in pcount:
#     try:
#         final_files[p] = sample(pcount[p], 1000)
#     except:
#         final_files[p] = sample(pcount[p], 430)
    
    
#read randomly sampled data into a dataframe
rowlist = []
for p in final_files.keys():
    party = p.replace("_", "").replace(".csv", "")
    for d in final_files[p]:
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
                    
                row = [sn, text, party, language]
                rowlist.append(row)

                
frame = pd.DataFrame(rowlist, columns=['screen_name','text','party', 'language'])
frame = frame.dropna(subset=['text'])


