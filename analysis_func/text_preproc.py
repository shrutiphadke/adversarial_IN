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


#text preproessing - filter engligh, hindi, marathi stop words, remove puncts, hash, mentions, urls, weird spaces etc.
stopwords = []

stopwordfiles = glob.glob("/home/phadke/ONR/ONR/lite_data/stopwords/*.txt")
for s in stopwordfiles:
    with open(s, "r") as sfile:
        for line in sfile:
            stopwords.append(line.strip())

            
emoji_pattern = re.compile("["
        u"\U0001F600-\U0001F64F"  # emoticons
        u"\U0001F300-\U0001F5FF"  # symbols & pictographs
        u"\U0001F680-\U0001F6FF"  # transport & map symbols
        u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
        u"\U0001F1F2-\U0001F1F4"  # Macau flag
        u"\U0001F1E6-\U0001F1FF"  # flags
        u"\U0001F600-\U0001F64F"
        u"\U00002702-\U000027B0"
        u"\U000024C2-\U0001F251"
        u"\U0001f926-\U0001f937"
        u"\U0001F1F2"
        u"\U0001F1F4"
        u"\U0001F620"
        u"\u200d"
        u"\u2640-\u2642"
        "]+", flags=re.UNICODE)




remove_urls = lambda x: re.sub("http(.+)?(\W|$)", ' ', x)
remove_RT = lambda x: x.replace("RT ", "")
remove_mentions = lambda x: re.sub("@\S+", '', x)
remove_hashtags = lambda x: re.sub("#\S+", '', x)
remove_digits = lambda x: re.sub("\d+", "", x)
remove_punct = lambda x: re.sub("!|\||\%|\.|\-|\/|:|…|,|\?|।+|'+", "", x)
remove_emojis = lambda x: emoji_pattern.sub("", x)
normalize_spaces = lambda x: re.sub("[\n\r\t ]+", ' ', x)
remove_stop = lambda x: " ".join(i for i in x.lower().split() if i not in stopwords)


preproc_text = lambda x: remove_stop(
                            normalize_spaces(
                                remove_emojis(
                                    remove_punct(
                                        remove_digits(
                                            remove_hashtags(
                                                remove_mentions(
                                                    remove_RT(
                                                        remove_urls(x)))))))))
