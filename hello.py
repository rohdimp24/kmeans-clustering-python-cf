from flask import Flask
import os
import pandas as pd
import scipy as sp
import numpy as np
import re
import csv
import json
app = Flask(__name__)

port = int(os.getenv("PORT", 64781))


def getIntiialize():
	fname = "normalized_cases_jim.txt"
	with open(fname, 'r') as myfile:
		data = myfile.read()
	data = data.split('---------BREAK---------')
	cases = [case.strip() for case in data]
	print(cases[1:10])
	print(len(cases))
	lengthCases = len(cases)
	lines = cases

	stopwordsFile = open('stopwordsss.txt', 'r')
	stopwords = stopwordsFile.read()
	stopwordList = stopwords.split(",")

	return (lines, stopwordList)


''' Utility functions'''
def getKeywordIndexInVocabulary(vocab,keyword):
    idx = list(vocab).index(keyword)
    return(idx)


def getDocumentsContainingKeywords(countDtm,vocab,keyword):
    #GET THE COLUMN NUM OF THE KEYWORD
    keywordIndex=getKeywordIndexInVocabulary(vocab,keyword)
    tt=list(countDtm[:,keywordIndex]>0)
    res=[]
    for idx,w in enumerate(tt):
        if(w==True):
            res.append(idx)
    return(res)

def getKeywordsOfDocument(dtm,vocab,docNo):
    nn=np.flatnonzero(dtm[docNo,])
    print(nn)
    #print(nn[1])
    #get the corresponding word from the vocabulary list
    return(vocab[nn].tolist())


def getFreqOfKeyword(freqDict,keyword):
    for key in freqDict:
        if(key['word']==keyword):
            return(key['count'])

def getNumberOfWordsForCluster(km, clusterNum, N):
	ret = np.unique(km.labels_, return_counts=True)
	clusterSize = ret[1][clusterNum]
	totalWords = float(clusterSize / N) * 100
	print(totalWords)
	if (totalWords < 10):
		return (10)
	return (int(totalWords) + 1)





#/<startCaseNumber>/<endCaseNumber>
@app.route('/')
def getVectorized():
	lines, stopwordList = getIntiialize()
	startIndex = 0
	endIndex = 4000
	# now we need to vectorize the corpus
	from sklearn.feature_extraction.text import TfidfVectorizer
	vectorizer = TfidfVectorizer(min_df=0.006, stop_words=stopwordList, strip_accents='unicode', norm='l2',
	 							 sublinear_tf=True)
	# # maximum is 4997
	tfSparseMatrix = vectorizer.fit_transform(lines[startIndex:endIndex])
	tfDtm = tfSparseMatrix.toarray()
	tfDtm = np.array(tfDtm)


	##count vectorizer
	from sklearn.feature_extraction.text import CountVectorizer
	count_vect = CountVectorizer(min_df=0.006, stop_words=stopwordList, strip_accents='unicode', binary=False)
	rawdtm = count_vect.fit_transform(lines[startIndex:endIndex])
	vocab = count_vect.get_feature_names()
	# convert the dtm to regular array
	countDtm = rawdtm.toarray()
	# convert the dtm to numpy array
	countDtm = np.array(countDtm)
	print(countDtm)
	# need to convert it to numpy array so that we can easily perform the operations on it
	vocab = np.array(vocab)


	##########
	freqsum = np.sum(countDtm, axis=0)

	# for each of the vocabulary word create a dictionary containing the count
	freqDict = []
	for idx, v in enumerate(vocab):
		freqDict.append({'word': v, 'count': freqsum[idx]})

	from sklearn.cluster import KMeans
	# the max_iter is how many iterations before the convergence is assumed
	# n_init is the number of times the algo is run
	K_Cluster = 4
	km = KMeans(n_clusters=K_Cluster, init='k-means++', max_iter=1000, n_init=10, verbose=False)
	# you need to call the km.fit_predict so that the kmeans cane be run and then each of the points can be assigned a cluster index
	km.fit_predict(tfSparseMatrix)

	clusterDistribution = np.unique(km.labels_, return_counts=True)
	numberOfClusters = len(clusterDistribution[0])
	# return (format(tfSparseMatrix.shape))
	return(str(numberOfClusters))




if __name__ == '__main__':
    app.run(host='0.0.0.0', port=port)
