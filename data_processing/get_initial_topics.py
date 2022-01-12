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
plist = ['_BJP.csv', '_INC.csv', '_AAP.csv']
pcount = defaultdict(list)
for d in datafiles:
    for p in plist:
        if p in d:
            pcount[p].append(d)
                
final_files = defaultdict(list)
for p in pcount:
    try:
        final_files[p] = sample(pcount[p], 1000)
    except:
        final_files[p] = sample(pcount[p], 430)
    
    
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

def startwithRT(text):
    if text.startswith("RT "):
        return 1
    
frame['is_RT'] = frame['text'].parallel_apply(lambda x: startwithRT(x))
frame = frame.loc[frame['is_RT']!=1]

print("cleaning data")
#text preproessing - filter engligh, hindi, marathi stop words, remove puncts, hash, mentions, urls, weird spaces etc.
frame['clean_text'] = frame['text'].parallel_apply(lambda x: preproc_text(x))


# keep text only in hi, mr and en for now
keep_lang = ['en','hi','mr']
lang_frame = frame.loc[frame['language'].isin(keep_lang)]


print("prepping corpus for topicmodel")
# prepare data for topic modeling
corp = lang_frame['clean_text'].tolist()
parties = lang_frame['party'].tolist()
corp_tokens = [c.split() for c in corp]
phrases = Phrases(corp_tokens, min_count=5, threshold=0.1)
tokes = [phrases[t] for t in corp_tokens]

BJP_data = []
INC_data = []
AAP_data = []

for i in range(len(tokes)):
    if parties[i]=='BJP':
        BJP_data.append(tokes[i])
    if parties[i]=='INC':
        INC_data.append(tokes[i])
    if parties[i]=='AAP':
        AAP_data.append(tokes[i])

        
# party wise topic model preproc


print("modeling topics")

topic_nums = [2, 4, 5, 6, 8, 10]
for tn in topic_nums:
    savefoldername = "ldamodels/" + str(datetime.datetime.now().strftime("%Y%m%d_%H%M")) + "/"

    BJP_id2word, BJP_corpus = prepare_corp(BJP_data)
    BJP_model = topicmodel(tn, BJP_corpus, BJP_id2word)
    print("saving BJP model and data in", savefoldername)
    save_modelanddata(BJP_model, BJP_id2word, BJP_corpus, savefoldername, "BJP")

    INC_id2word, INC_corpus = prepare_corp(INC_data)
    INC_model = topicmodel(tn, INC_corpus, INC_id2word)
    print("saving INC model and data in", savefoldername)
    save_modelanddata(INC_model, INC_id2word, INC_corpus, savefoldername, "INC")

    AAP_id2word, AAP_corpus = prepare_corp(AAP_data)
    AAP_model = topicmodel(tn, AAP_corpus, AAP_id2word)
    print("saving AAP model and data in", savefoldername)
    save_modelanddata(AAP_model, AAP_id2word, AAP_corpus, savefoldername, "AAP")