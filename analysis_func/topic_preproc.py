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


def prepare_corp(datalist):
    id2word = corpora.Dictionary(datalist)
    corpus = [id2word.doc2bow(text) for text in datalist]
    return id2word, corpus