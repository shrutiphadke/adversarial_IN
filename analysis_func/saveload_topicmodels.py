import datetime
import os
import time
import sys
import pickle as pkl
from gensim import models


def save_modelanddata(model, id2word, corpus, foldername, partyname):
    basepath = "./intermediate_models/"
    newfolder = basepath + foldername
    if not os.path.exists(newfolder):
        os.makedirs(newfolder)
        
    modelpath = newfolder + partyname + "_model.model"
    model.save(modelpath)
    
    id2wordpath = newfolder + partyname + "_id2word.pkl"
    
    with open(id2wordpath, "wb") as ifile:
        pkl.dump(id2word, ifile)
        
    corpuspath = newfolder + partyname + "_corpus.pkl"
    with open(corpuspath, "wb") as cfile:
        pkl.dump(corpus, cfile)
        
        
        
def load_modelanddata(foldername, partyname):
    basepath = "./intermediate_models/"
    newfolder = basepath + foldername
    modelpath = newfolder + partyname + "_model"
    model = models.LdaModel.load(modelpath)
    
    id2wordpath = newfolder + partyname + "_id2word.pkl"
    corpuspath = newfolder + partyname + "_corpus.pkl"
    
    with open(id2wordpath, "rb") as ifile:
        id2word = pkl.load(ifile)

    with open(corpuspath, "rb") as cfile:
        corpus = pkl.load(cfile)
        
    return model, id2word, corpus
        
    
