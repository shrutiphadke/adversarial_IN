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


def topicmodel(n_topics, corpus, id2word, workers=128):
    if workers>1:
        model = gensim.models.ldamulticore.LdaMulticore(corpus=corpus,
                                                        id2word=id2word,
                                                        workers=128,
                                                        num_topics=n_topics,
                                                        random_state=100,
                                                        chunksize=100,
                                                        passes=10,
                                                        per_word_topics=False)
        return model
    
    
    else:
        model = gensim.models.ldamodel.LdaModel(corpus=corpus,
                                                id2word=id2word,
                                                num_topics=n_topics,
                                                random_state=100,
                                                chunksize=100,
                                                passes=10,
                                                per_word_topics=True)
        return model
    
    
    
def findNtopics(corpus, id2word, workers=128):
    fakevar=0
    #fill out later
    
    