from flask import Flask
import os
import numpy as np
import json
import requests
from sklearn.cluster import KMeans

#from kmeans-python import TokenizeCases


app = Flask(__name__)

port = int(os.getenv("PORT", 64781))

#for some reason the user has to be postgres and not root
'''Db connection'''
#conn =psycopg2.connect(database='jim', user='postgres', password='root', host='127.0.0.1', port='5432')
conn=''

# class Object:
#     def toJSON(self):
#         return json.dumps(self, default=lambda o: o.__dict__,
#             sort_keys=True, indent=4)



'''
Function Name:getIntiialize
Purpose: Reading teh normalized cases based on teh equipment type
Variables:
    -conn: the connection object
    -equipmentType: The category of cases

'''
def getIntiialize(conn,equipmentType):
    # cur = conn.cursor()
    # # cur.mogrify("Select id,description from cases.smartsignal_jim_allfields where smartsignal_jim_allfields.equipmentType=%s",(equipmentType,))
    # cur.execute("Select id,\"normalizedCase\" from cases.smartsignal_normalized_case where \"equipmentType\"=%s",
    #             (equipmentType,))
    #
    # rows = cur.fetchall()
    #
    # cases = {}
    # # display the rows
    # for row in rows:
    #     # print (row[0],row[1])
    #     # cases[row[0]]={"original":row[1]}
    #     cases[row[0]] = row[1]
    # url = 'https://data-ingestion-api.run.aws-usw02-pr.ice.predix.io/'
    # # url='https://text-mining-212470820-02.run.asv-pr.ice.predix.io/processData/30?Tenant=PredixForum&Token=Token'
    #
    # headers = {'content-type': "application/x-www-form-urlencoded",
    #            'Authorization': "Basic aW5nZXN0b3IuZWExYTAyNDUtMTc1OC00ZTY2LWFiZjAtZjI1Mzk1NmE3ODIwLmY4ZGFhYTUwLTBhOGYtNGU1ZS1hZjJhLTkyNjEyMDQwNGJhMTo="
    #            }
    #
    # resp = requests.post('https://4126b27b-6860-48ee-9dc1-9cba313eac9f.predix-uaa.run.asv-pr.ice.predix.io/oauth/token',
    #                      data='grant_type=password&username=f743b7ef-42df-4d7e-89dd-90dc3b53b0ac_ingestor&password=Pa55w0rd',
    #                      headers=headers)


    #authString = json.loads(resp.text)

    print("------------------")
    #print(authString)

    #now using the authstring find out the cases



    fname = "normalized_cases_jim.txt"
    with open(fname, 'r') as myfile:
        data = myfile.read()
    data = data.split('---------BREAK---------')
    cases = [case.strip() for case in data]
    #print(cases[1:10])
    #print(len(cases))
    #lengthCases = len(cases)
    #lines = cases

    #cases = pickle.load(open("casesDict.p", "rb"))

    stopwordsFile = open('stopwordsss.txt', 'r')
    stopwords = stopwordsFile.read()
    stopwordList = stopwords.split(",")

    return (cases, stopwordList)



''' Utility functions'''


'''
Function Name:getKeywordIndexInVocabulary
Purpose: Given the keywprd we want to know what is its index in the vocab list
Variables:
    -vocab: the vocabulary
    -keyword: for which the index needs to be found
'''
def getKeywordIndexInVocabulary(vocab,keyword):
    idx = list(vocab).index(keyword)
    return(idx)



'''
Function Name:getDocumentsContainingKeywords
Purpose: Given the keywprd we want to know in which documents it has appeared. We will use the countDtm that is a matrix of documents and the keywords
for this task. The vocabulary is required to find out the index of the keyword. the same index is the column index in the countDtm
Variables:
    -vocab: the vocabulary
    -keyword: for which the documents needs to be found
    -countDtm: document -term matrix
'''
def getDocumentsContainingKeywords(countDtm,vocab,keyword):
    #GET THE COLUMN NUM OF THE KEYWORD
    keywordIndex=getKeywordIndexInVocabulary(vocab,keyword)
    tt=list(countDtm[:,keywordIndex]>0)
    res=[]
    for idx,w in enumerate(tt):
        if(w==True):
            res.append(idx)
    return(res)

'''
Function Name:getKeywordsOfDocument
Purpose: Given the document we want to find out all the keywords. Basically using the dtm we find the coulm indexes
which are non zero for the particular document and then using the vocabulary we get the words that are in those indexes
Variables:
    -vocab: the vocabulary
    -docNo: the document for which the keywords need to be found
    -dtm: the document-term matrix
'''
def getKeywordsOfDocument(dtm,vocab,docNo):
    nn=np.flatnonzero(dtm[docNo,])
    print(nn)
    #print(nn[1])
    #get the corresponding word from the vocabulary list
    return(vocab[nn].tolist())



'''
Function Name:getFreqOfKeyword
Purpose: How many times a particular word has appeared in the corpus(not document frequncy as we will consider the
absolute number and not the binary)
Variables:
    -freqDict: the dictionary containing the frequency of all the keywords. This is created at the time of creating the countDTM
    -keyword:  the word for which the frequncy needs to be found

'''
def getFreqOfKeyword(freqDict,keyword):
    for key in freqDict:
        if(key['word']==keyword):
            return(key['count'])



'''
Function Name:getNumberOfWordsForCluster
Purpose: How many words to be shown for each cluster. This is based on the proportion of the size of cluster. Basically what percentage of
 documents are in a cluster. Then out of 100 words in the wordscloud , how many words come from this cluster
Variables:
    -km: The kmeans object containing the results of algorithm
    -clusterNum:  the cluster id for which we want to determine the number of words
    -N: Number of documents which were considered for clustering
'''
def getNumberOfWordsForCluster(km, clusterNum, N):
	ret = np.unique(km.labels_, return_counts=True)
	clusterSize = ret[1][clusterNum]
	totalWords = float(clusterSize / N) * 100
	print(totalWords)
	if (totalWords < 10):
		return (10)
	return (int(totalWords) + 1)


# casesDict, stopwordList = getIntiialize(conn,equipmentType)
# print(casesDict)
# lines = []
# countToCaseIdMap={}
# # # maximum is 4997
# count=0
# for key in casesDict:
#     lines.append(casesDict[key])  # now we need to vectorize the corpus
#     countToCaseIdMap[count]=key
#     count=count+1
#
# print(lines)
# print(countToCaseIdMap)

# def getNumberOfWordsForCluster(km,clusterNum,N):
#     ret=np.unique(km.labels_, return_counts=True)
#     clusterSize=ret[1][clusterNum]
#     totalWords=float(clusterSize/N)*100
#     print(totalWords)
#     if(totalWords<10):
#         return(10)
#     return(int(totalWords)+1)


'''
Function Name:getVectorized
Purpose: Thsi is the main workhorse. The following are the tasks it performs:
1. countToCaseIdMap: A map containing the index of the document in the resultset (casesDict) and the actual caseid.
This caseId is the primary key to retrieve the original case from the db at any time
2.tfSparseMatrix: this is the tf-idf document-term matrix that will be used for clustering
3.countDtm: This is the document-term matrix without tf-idf. we will use it for findong frequecy of keywords, vocab etc
4.vocab: list of keywords obtained from countDtm


Variables:
    -conn: The postgres connection  object
    -equipmentType: WIND_TURBINE, STEAM_TURBINE etc
'''
def getVectorized(conn,equipmentType):

    casesDict, stopwordList = getIntiialize(conn,equipmentType)
    print(casesDict)
    lines = casesDict
    countToCaseIdMap={}
    # # maximum is 4997
    count=0
    for key in range(len(casesDict)):
        #lines.append(casesDict[key])  # now we need to vectorize the corpus
        countToCaseIdMap[count] = key
        count=count+1

    from sklearn.feature_extraction.text import TfidfVectorizer
    vectorizer = TfidfVectorizer(min_df=0.006, stop_words=stopwordList, strip_accents='unicode', norm='l2',
                                 sublinear_tf=True)


    tfSparseMatrix = vectorizer.fit_transform(lines)
    tfDtm = tfSparseMatrix.toarray()
    tfDtm = np.array(tfDtm)


    from sklearn.feature_extraction.text import CountVectorizer
    count_vect = CountVectorizer(min_df=0.006, stop_words=stopwordList, strip_accents='unicode', binary=False)
    rawdtm = count_vect.fit_transform(lines)
    vocab = count_vect.get_feature_names()
    # convert the dtm to regular array
    countDtm = rawdtm.toarray()
    # convert the dtm to numpy array
    countDtm = np.array(countDtm)
    print(countDtm)
    # need to convert it to numpy array so that we can easily perform the operations on it
    vocab = np.array(vocab)


    ##########
    # freqsum = np.sum(countDtm, axis=0)
    #
    # # for each of the vocabulary word create a dictionary containing the count
    # freqDict = []
    # for idx, v in enumerate(vocab):
    #     freqDict.append({'word': v, 'count': freqsum[idx]})

    # from sklearn.cluster import KMeans
    # # the max_iter is how many iterations before the convergence is assumed
    # # n_init is the number of times the algo is run
    # K_Cluster = 3
    # km = KMeans(n_clusters=K_Cluster, init='k-means++', max_iter=1000, n_init=10, verbose=False)
    # # you need to call the km.fit_predict so that the kmeans cane be run and then each of the points can be assigned a cluster index
    # km.fit_predict(tfSparseMatrix)
	# # return (format(tfSparseMatrix.shape))
	#return("skjdhkjsd")


    return(lines,countToCaseIdMap,tfSparseMatrix,count_vect,countDtm)


'''
Function Name:performKmeans
Purpose: performs kmeans clustering using the tf-idf matrix


Variables:
    -tfSparseMatrix: tf-idf matrix

'''
def performKmeans(tfSparseMatrix):

    # the max_iter is how many iterations before the convergence is assumed
    # n_init is the number of times the algo is run
    K_Cluster = 3
    km = KMeans(n_clusters=K_Cluster, init='k-means++', max_iter=1000, n_init=10, verbose=False)
    # you need to call the km.fit_predict so that the kmeans cane be run and then each of the points can be assigned a cluster index
    km.fit_predict(tfSparseMatrix)
    return(km)

'''
Function Name:getWordMapForCluster
Purpose: This function will create a map of the keyword and the frequecy of its ocurence in that cluster.
It takes as an input a set of documents that have appeared in the cluster. Now for each document will find out the number of times
a particular word has appeared
It will finally sort the word list as per the frequency and return


Variables:
    -documents: A list of document ids that are to be considered
    -countDtm: This is the Document-term matrix whose cells will tell the frequency of the word in the document
    -vocab: a list of keywords
'''
def getWordMapForCluster(documents, countDtm, vocab):
    wordMap = {}
    #for each document find out which all keywords appear
    for d in documents:
        #print(d)
        # this gives the index of columns and hence keywords
        nn = np.flatnonzero(countDtm[int(d),])

        # find the word and the count on that index. Using the vicabulary you will know the word
        for ind in nn:
            if (vocab[ind] not in wordMap.keys()):
                wordMap[vocab[ind]] = countDtm[int(d), ind]
            else:
                wordMap[vocab[ind]] += countDtm[int(d), ind]

    #sort the map as per the frequency of the words
    from operator import itemgetter
    sortedWordMap = sorted(wordMap.items(), key=itemgetter(1), reverse=True)
    return (sortedWordMap)



'''
Function Name:getwordList
Purpose: This function will create the final list of wordcreate a map of the keyword and the frequecy of its ocurence in all the  cluster.
It will iteratively find out the wordlist for each cluster. Then pick up the top n words (based on the size of cluter) and then create a final
list
Some of the words can be common in all the clusters (multiple words list), so they will be merged

Variables:
    -km: kmeans clustering object
    -N: The number of documents in teh corpus
    -k: The number of clusters
    -casesPerCluster: distribution of cases per cluster
    -countDtm: the document term matrix
    -vocab: the list of the keywords
'''
def getwordList(km, N, k,casesDistributionPerCluster, countDtm, vocab):
    finalList = []
    numberOfClusters = k
    numberOfCases = N
    for clusterNum in range(numberOfClusters):
        #print(clusterNum)
        n = getNumberOfWordsForCluster(km, clusterNum, numberOfCases)
        # get wordmap for a cluster
        wordList = getWordMapForCluster(casesDistributionPerCluster[clusterNum].split(','), countDtm, vocab)
        finalList.append(wordList[:n])

    print(finalList)
    '''We have got a list of list that needs to be flattened'''

    flatten = lambda l: [item for sublist in l for item in sublist]
    flattenFinalList = flatten(finalList)
    print(flattenFinalList)

    '''some of the words in the wordlist can be repeatitive so we will meger them together'''
    wordListForWordCloud = {}
    for listItem in flattenFinalList:

        key = listItem[0]
        value = listItem[1]
        if (key not in wordListForWordCloud.keys()):
            wordListForWordCloud[key] = value
        else:
            wordListForWordCloud[key] += value

    print(wordListForWordCloud)

    '''Sort the list and print. the list containe the word ans its frequencty'''
    from operator import itemgetter
    sortedWordMap = sorted(wordListForWordCloud.items(), key=itemgetter(1), reverse=True)

    return (sortedWordMap)


'''
Function Name:getWordCloudListJson
Purpose: returns a JSON of wordlist that can be used to generate a wordcloud

Variables:
    -km: kmeans clustering object
    -N: The number of documents in teh corpus
    -k: The number of clusters
    -casesDistributionPerCluster: distribution of cases per cluster
    -countDtm: the document term matrix
    -vocab: the list of the keywords
'''
def getWordCloudListJson(km,N,k,casesDistributionPerCluster,countDtm,vocab):
    wl = getwordList(km,N, k, casesDistributionPerCluster, countDtm, vocab)
    wlJson = []
    for key in wl:
        vocabWord = key[0]
        vocabFreq = key[1]
        wlJson.append({"keyword":vocabWord,"frequency":str(vocabFreq)})

    return(json.dumps(wlJson))


'''
Function Name:getWordCloudListWithCasesJson
Purpose: returns a JSON of wordlist along with which all cases are associated with that word. It will also highlight
 the section of the case where the word has appeared

Variables:
    -km: kmeans clustering object
    -N: The number of documents in teh corpus
    -k: The number of clusters
    -casesDistributionPerCluster: distribution of cases per cluster
    -countDtm: the document term matrix
    -vocab: the list of the keywords
'''
def getWordCloudListWithCasesJson(km, N, k, casesDistributionPerCluster, countDtm, vocab,countToCaseIdMap,cases):
    wlWithCases = []
    wl = getwordList(km, N,k, casesDistributionPerCluster, countDtm, vocab)

    for key in wl:
        vocabWord = key[0]

        index = vocab.index(vocabWord)
        print(vocabWord)
        # get all the rows for which the column at this index is nonzero
        nn = np.flatnonzero(countDtm[:, index])
        caseIds = []
        count=0
        for n in nn:
            caseIds.append({'caseId': str(countToCaseIdMap[n]),
                            'description': cases[n].replace(vocabWord, "<class='highlight'>" + vocabWord + "</class>")})
            # caseIds.append(countToCaseIdMap[n])
            count+=1
            if(count>10):
                break
        wlWithCases.append({'tag':vocabWord,'size': str(key[1]), 'caseIds': caseIds})
    finalResult = {}
    finalResult["name"] = "WordCloud"
    finalResult["errorCode"] = "Null"
    finalResult["wordFrequencyModel"] = wlWithCases

    return (json.dumps(finalResult))




'''
Function Name:getCasesDistributionPerCLuster
Purpose: This function will find out the distribution of the cases in the cluster. Basically which cases belon to which cluster

Variables:
    -km: kmeans clustering object

'''
#which all cases belong to which cluster
def getCasesDistributionPerCluster(km):
    casesPerCluster = {}
    for i, cluster in enumerate(km.labels_):
        if cluster not in casesPerCluster.keys():
            casesPerCluster[cluster] = str(i)
        else:
            casesPerCluster[cluster] += "," + str(i)
    return(casesPerCluster)



'''
Function Name:getFreqDistribution
Purpose: How many times a particular word has appeared in the corpus(not document frequncy as we will consider the
absolute number and not the binary. A dictionary or word and its overall frequency is created

Variables:
    -countDtm: document-term matrix containing all teh keywords in the columns. the cell determins the tf score

'''
def getFreqDistribution(countDtm,vocab):
    freqsum = np.sum(countDtm, axis=0)

    # for each of the vocabulary word create a dictionary containing the count
    freqDict = []
    for idx, v in enumerate(vocab):
        freqDict.append({'word': v, 'count': freqsum[idx]})
    return(freqDict)

def printAsJSON(obj):
    return(json.dumps(obj))


'''The main code'''

@app.route('/')
def getWordListAPi():
    equipmentType="STEAM_TURBINE"
    #conn=''
    cases,countToCaseIdMap,tfSparseMatrix,count_vect,countDtm=getVectorized(conn,equipmentType)
    km=performKmeans(tfSparseMatrix)

    #return("sjdjksdk")
    '''Find out how many documents are  in each cluster'''
    clusterDistribution = np.unique(km.labels_, return_counts=True)
    numberOfClusters=len(clusterDistribution[0])

    print(numberOfClusters)
    #return(str(numberOfClusters))

    # '''the vocabulary of the words'''
    vocab=count_vect.get_feature_names()
    print(vocab)
    #return(printAsJSON(vocab))
    # ''' How the documents are distrbiuted in each cluster..which all cases lie in cluster 1 ..3'''
    #
    casesDistributionPerCluster=getCasesDistributionPerCluster(km)
    print(casesDistributionPerCluster[1])
    #return(printAsJSON(casesDistributionPerCluster[1]))

    '''
    Get the word list in JSON
    '''
    wl=getWordCloudListJson(km,len(cases),3,casesDistributionPerCluster,countDtm,vocab)
    print(wl)
    #
    # return(wl)

    # ''''
    # Now find out how the various cases in which the cases have appeared.
    # '''
    wlWithCases=getWordCloudListWithCasesJson(km,len(cases),3,casesDistributionPerCluster,countDtm,vocab,countToCaseIdMap,cases)
    return(wlWithCases)

    # # dump the json to a file
    #outF = open("kmeansOutput.json", "w")
    # outF.write(json.dumps(wlWithCases))
    # outF.close()
    #



if __name__ == '__main__':
    app.run(host='0.0.0.0', port=port)


#create a map

