from __future__ import division
from flask import Flask
from flask import request

import os
import numpy as np
import re
import json
import csv
from sklearn.cluster import KMeans
import requests
#from kmeans-python import TokenizeCases


app = Flask(__name__)


port = int(os.getenv("PORT", 64782))

#session['isValidated']=False
isValidated=False

#conn = psycopg2.connect(database='jim', user='postgres', password='root', host='127.0.0.1', port='5432')


# cur = conn.cursor()
# cur.execute("Select id,description from cases.smartsignal_jim_allfields where \"equipmentType\"=%s",(equipmentType,))
# rows = cur.fetchall()
#
# cases={}
# #display the rows
# for row in rows:
#     print (row[0],row[1])
#     #cases[row[0]]={"original":row[1]}
#     cases[row[0]]=row[1]
#
# conn.commit()

#check_token = 'eyJhbGciOiJSUzI1NiIsImtpZCI6ImxlZ2FjeS10b2tlbi1rZXkiLCJ0eXAiOiJKV1QifQ.eyJqdGkiOiI3ZDJiMGIyNGU3YTI0MmUwODlkMTBiYWJkZDg1Mzk0YSIsInN1YiI6ImRpZ2l0YWxfaXByYy1zZXJ2aWNlX3Byb2QiLCJzY29wZSI6WyJhY3MucG9saWNpZXMucmVhZCIsInVhYS5yZXNvdXJjZSIsImFjcy5hdHRyaWJ1dGVzLnJlYWQiLCJvcGVuaWQiLCJwcmVkaXgtYWNzLnpvbmVzLmFlYmE4NWY3LTdlNjMtNGMyMS1hZDBjLWExYmJkMGIyNWQ0MS51c2VyIl0sImNsaWVudF9pZCI6ImRpZ2l0YWxfaXByYy1zZXJ2aWNlX3Byb2QiLCJjaWQiOiJkaWdpdGFsX2lwcmMtc2VydmljZV9wcm9kIiwiYXpwIjoiZGlnaXRhbF9pcHJjLXNlcnZpY2VfcHJvZCIsImdyYW50X3R5cGUiOiJjbGllbnRfY3JlZGVudGlhbHMiLCJyZXZfc2lnIjoiMWZmYjY4MjkiLCJpYXQiOjE0ODQ4MTUxNDQsImV4cCI6MTQ4NDgyMjM0MywiaXNzIjoiaHR0cHM6Ly9hOTliN2ZlZS1hNDk1LTQxNjEtODljMy1mYWE4MzA1NDYyN2QucHJlZGl4LXVhYS5ydW4uYXN2LXByLmljZS5wcmVkaXguaW8vb2F1dGgvdG9rZW4iLCJ6aWQiOiJhOTliN2ZlZS1hNDk1LTQxNjEtODljMy1mYWE4MzA1NDYyN2QiLCJhdWQiOlsiYWNzLmF0dHJpYnV0ZXMiLCJ1YWEiLCJwcmVkaXgtYWNzLnpvbmVzLmFlYmE4NWY3LTdlNjMtNGMyMS1hZDBjLWExYmJkMGIyNWQ0MSIsImRpZ2l0YWxfaXByYy1zZXJ2aWNlX3Byb2QiLCJvcGVuaWQiLCJhY3MucG9saWNpZXMiXX0.G00l8uALjnvV4E4UqeHOw3Zz0vK5cnp8pOuJY5P2ii5kSoVPhFspnpMF_0NiZSx7rKue6_D-YJPSYOmIIOFOFLK23Qc78PYgErtN5qQqBY6MtvCPdg7QRVAXha1znkJscPL414qXGW2hUAccqGV2OJF1CxOgVovt3zxlgvcDbWnnKTm44hbgD6-UqPuN4EH79VEW4T0QohMNKf9w1Wtd4fLNVeYDNK4O_A8_21uz8zdxzIlssZQrAeire05korRt1SIAPFp80RgXxzwCDIksdU9h_rhx3aYKZiYSVzRmYGXYytHGCFnTEJMpKl3SkEZem8W0gwojnhPy9XsxaYw3vg'



def tokenize_getResponseFromService(equipmentType,token):

    # uaaUrl = 'https://4126b27b-6860-48ee-9dc1-9cba313eac9f.predix-uaa.run.asv-pr.ice.predix.io/oauth/token'
    # # url='https://text-mining-212470820-02.run.asv-pr.ice.predix.io/processData/30?Tenant=PredixForum&Token=Token'
    #
    # headers = {'content-type': "application/x-www-form-urlencoded",
    #            'Authorization': "Basic aW5nZXN0b3IuZWExYTAyNDUtMTc1OC00ZTY2LWFiZjAtZjI1Mzk1NmE3ODIwLmY4ZGFhYTUwLTBhOGYtNGU1ZS1hZjJhLTkyNjEyMDQwNGJhMTo="
    #            }
    #
    # resp = requests.post(uaaUrl,
    #                      data='grant_type=password&username=f743b7ef-42df-4d7e-89dd-90dc3b53b0ac_ingestor&password=Pa55w0rd',
    #                      headers=headers)
    #
    # authString = json.loads(resp.text)
    #
    # access_token = authString['access_token']

    access_token=token
    #TODO:all the above code will be commented and it will be just access_token=token

    serviceUrl = 'https://km-knowledge-base.run.asv-pr.ice.predix.io/v1/cases/kmbase/filteredentities?type=cases&equipmentType=' + equipmentType
    # url='https://text-mining-212470820-02.run.asv-pr.ice.predix.io/processData/30?Tenant=PredixForum&Token=Token'

    headers = {'content-type': "application/json",
               'Authorization': "bearer " + access_token,
               'tenant': "f743b7ef-42df-4d7e-89dd-90dc3b53b0ac"
               }

    resp = requests.get(serviceUrl, headers=headers)
    return(resp)


'''
For now I have hard coded . this will be the same token as passed by calling person
'''
def getAllEquipmentsFromService(token):

    #TODO:the access_token will be equal to the token and the services url will change
    #access_token='eyJhbGciOiJSUzI1NiIsImtpZCI6ImxlZ2FjeS10b2tlbi1rZXkiLCJ0eXAiOiJKV1QifQ.eyJqdGkiOiI0ZDQ5MjMxOWY2MjA0Y2JlYjYwOGIyZWUyMzU1OGQ5MiIsInN1YiI6IjQwNzc1MmYwLTc4ZTktNGQyOC05YTJkLTM2NGUxMjQxMmE3MSIsInNjb3BlIjpbInBhc3N3b3JkLndyaXRlIiwib3BlbmlkIl0sImNsaWVudF9pZCI6ImluZ2VzdG9yLmVhMWEwMjQ1LTE3NTgtNGU2Ni1hYmYwLWYyNTM5NTZhNzgyMC5mOGRhYWE1MC0wYThmLTRlNWUtYWYyYS05MjYxMjA0MDRiYTEiLCJjaWQiOiJpbmdlc3Rvci5lYTFhMDI0NS0xNzU4LTRlNjYtYWJmMC1mMjUzOTU2YTc4MjAuZjhkYWFhNTAtMGE4Zi00ZTVlLWFmMmEtOTI2MTIwNDA0YmExIiwiYXpwIjoiaW5nZXN0b3IuZWExYTAyNDUtMTc1OC00ZTY2LWFiZjAtZjI1Mzk1NmE3ODIwLmY4ZGFhYTUwLTBhOGYtNGU1ZS1hZjJhLTkyNjEyMDQwNGJhMSIsImdyYW50X3R5cGUiOiJwYXNzd29yZCIsInVzZXJfaWQiOiI0MDc3NTJmMC03OGU5LTRkMjgtOWEyZC0zNjRlMTI0MTJhNzEiLCJvcmlnaW4iOiJ1YWEiLCJ1c2VyX25hbWUiOiJmNzQzYjdlZi00MmRmLTRkN2UtODlkZC05MGRjM2I1M2IwYWNfaW5nZXN0b3IiLCJlbWFpbCI6ImY3NDNiN2VmLTQyZGYtNGQ3ZS04OWRkLTkwZGMzYjUzYjBhY19pbmdlc3RvckBzdHVmLXJjLnJ1bi5hc3YtcHIuaWNlLnByZWRpeC5pbyIsImF1dGhfdGltZSI6MTQ4NDgwNDM4NiwicmV2X3NpZyI6IjgzM2U3MzFkIiwiaWF0IjoxNDg0ODA0Mzg2LCJleHAiOjE0ODQ4OTA3ODYsImlzcyI6Imh0dHBzOi8vNDEyNmIyN2ItNjg2MC00OGVlLTlkYzEtOWNiYTMxM2VhYzlmLnByZWRpeC11YWEucnVuLmFzdi1wci5pY2UucHJlZGl4LmlvL29hdXRoL3Rva2VuIiwiemlkIjoiNDEyNmIyN2ItNjg2MC00OGVlLTlkYzEtOWNiYTMxM2VhYzlmIiwiYXVkIjpbInBhc3N3b3JkIiwiaW5nZXN0b3IuZWExYTAyNDUtMTc1OC00ZTY2LWFiZjAtZjI1Mzk1NmE3ODIwLmY4ZGFhYTUwLTBhOGYtNGU1ZS1hZjJhLTkyNjEyMDQwNGJhMSIsIm9wZW5pZCJdfQ.wNIirL2jISy_iPvqSPfs7KqZ5JiqrkEkZHTK2vFjTLQeyEVD6Rf6w2XjCB8UFLDpQ3UCf5dfMzpZu4Yh-cocVDWM5xMx73AjwBizJNCKsXQa08prrMwUlBwTZlU7F19e0_vd0kkUgZ4SZHALE89S_enKItv0-69YQ89n4lo1pYFT_28Hh5uXeKqkQ9lukII0ltvH_2xgf6zHs-LKLE2eah8QWvBDkWg4l1kKGsKrSqdsf8P8HHdx1Siuil7UaUqhb7Bsc4Uq6-Eg78nD7ANr_xhfra1F16iLRfLspbWsqlm3gxxfEst75ZtCYEzQ0-AVrfuvMA-Y2976Qv75CX-fow'
    access_token=token
    headers = {'content-type': "application/json",
               'Authorization': "bearer " + access_token,
               'tenant': "f743b7ef-42df-4d7e-89dd-90dc3b53b0ac"
               }
    resp = requests.get("https://km-knowledge-base.run.asv-pr.ice.predix.io/v1/cases/kmbase/equipmentTypes?type=cases"
                        , headers=headers)



    return(resp.text)


def validateTokenService(token):
    global isValidated
    if(isValidated==False):
    #global isValidated']==False):

        url = "https://km-security.run.asv-pr.ice.predix.io/v1/km/validateToken"
        headers = {'content-type': "application/json",
                   'Authorization': "bearer " + token
                   }
        resp = requests.get(url, headers=headers)
        #print(resp.text)
        status = json.loads(resp.text)
        if ("data" in status):
            print("success")
            isValidated=True
            return ("success")
        else:
            print("failure")
            return ("failure")
    else:
        print("success short")
        return("success")

'''
Function Name:tokenize_getCasesFromSource
Purpose: This is used to get the cases from the KBasS service. The KBaaS service will return a json. The code will parse
the JSON to fetch specific sections. for now it is just the symptoms or the case description
Variables:
    -equipmentType: which kind of equipment wind turbine, steam turbine etc

'''
def tokenize_getCasesFromSource(equipmentType,token):

    #print("token passed.................",token.strip())
    status=validateTokenService(token)
    print(status)
    if(status=="success"):
        resp=tokenize_getResponseFromService(equipmentType,token)
        #print(resp.text)
        cases = {}
        firstLevel = json.loads(resp.text)
        count = 0
        for firstLevelKeys in firstLevel:
            caseId = firstLevelKeys['caseId']
            # json.loads will convert the string to dictionary so that we can access using a key
            secondLevel = json.loads(firstLevelKeys['data'])
            #print("----")
            #print(caseId, secondLevel['symptoms'])
            cases[caseId] = secondLevel['symptoms']
            count = count + 1
        #print(cases)
        return(cases)


'''
Function Name:tokenize_getCasesAndDictionaryFromSource
Purpose: This is sused to fetch teh cases and the dictionary from teh source. The dictionary has been seperately genertaed
Variables:
    -equipmentType: which kind of equipment wind turbine, steam turbine etc

'''
def tokenize_getCasesAndDictionaryFromSource(equipmentType,token):
	# fname = "all_jim_case_large.txt"
	# with open(fname, 'r') as myfile:
	# 	data = myfile.read()
    #
	# data = data.split('-------BREAK--------')
	# cases = [case.strip() for case in data]
	# #print(cases[1:10])
    cases=tokenize_getCasesFromSource(equipmentType,token)
    dictFile = "dict.csv"
    unigramDict = {}
    ngramDict = {}
    with open(dictFile, 'r') as csvfile:
        posReader = csv.reader(csvfile, delimiter=',', quotechar='|')
        for row in posReader:
            # print(row)
            # check for the presenc eof space
            if (len(row[0].split()) > 1):
                str = row[0]
                str = str.replace('"', '')
                ngramDict[str] = row[1].replace('"', '')
            else:
                str = row[0].replace('"', '')
                unigramDict[str] = row[1].replace('"', '')

    return (cases,unigramDict,ngramDict)



'''
Function Name:tokeinze_my_in_array
Purpose: This is used to find if the word exist in the dictionary (unigram/ngrams)
Variables:
    -word: token which has to be compared for its existence
    -dictArr: The dictionary that will contain the keywords
    -isNgram: Flag to check which particuar dictionary : unigram or ngram will be used.
'''
def tokeinze_my_in_array(word, dictArr, isNgram):
    # print(type(word))
    for idx,key in enumerate(dictArr):
        # print(key,word)
        if(isNgram==True):
            if(dictArr[key]==word):
                return(dictArr[key])
        else:
            if(key==word):
                #print(word)
                return(dictArr[key])


'''
Function Name:tokenize_getNormalizedWord
Purpose: This is used to get the bigram/trigram using the dictionary
Variables:
    -word: token which has to be compared for its existence
    -ngramDict: The dictionary that will contain the bigram keywords

'''
def tokenize_getNormalizedWord(word, ngramDict):
    retWord = tokeinze_my_in_array(word, ngramDict, True)
    if (retWord):
        retWord = retWord.replace(" ", "_", )
        retWord=retWord.replace('"','')
        retWord=retWord+" "
    return (retWord)




'''
Function Name:tokenize_getNormalizedCases
Purpose: This is the main function that will find out the quadgram, trigram, bigram, unigrams
Variables:
    -equipmentType:.which kind of equipment wind turbine, steam turbine etc
'''

def tokenize_getNormalizedCases(equipmentType,token):

    cases,unigramDict,ngramDict=tokenize_getCasesAndDictionaryFromSource(equipmentType,token)

    #print(unigramDict)
    # for key,value in cases.items():
    #     print(cases[key])
    arrUnigramFiltered = {}
    #startCaseNumber = 0
    #endCaseNumber = 100
    for key in cases:
        # print("before {}",case)
        case = cases[key]
        #print(case)
        case = case.strip();
        case = re.sub('/[^A-Za-z0-9 _\-\+\&\,\#]/', '', case)
        case = case.replace('"', ' ')
        case = case.replace('\"', ' ')
        case = case.replace('>', ' ')
        case = case.replace('@', ' ')
        case = case.replace('<', ' ')
        case = case.replace(':', ' ')
        case = case.replace('.', ' ')
        case = case.replace('(', ' ')
        case = case.replace(')', ' ')
        case = case.replace('[', ' ')
        case = case.replace(']', ' ')
        case = case.replace('_', ' ')
        case = case.replace(',', ' ')
        case = case.replace('#', ' ')
        case = case.replace('-', ' ')
        case = case.replace('/', ' ')
        case = case.replace('"', ' ')
        case = case.replace('\n', ' ')
        case = case.replace('\r', ' ')
        case = case.replace('~', ' ')
        case = case.replace('%', ' ')
        case = case.replace('$', ' ')
        case = case.replace('!', ' ')
        case = case.replace('*', ' ')

        case = re.sub(r'\d+', ' ', case)

        # print("after {}",case)
        arrTempTerms = case.split(" ")
        #print(arrTempTerms)
        str = ''
        for term in arrTempTerms:
            largestStringFound = ''
            firstword = term.lower()
            tempword = firstword
            # check if the word is present in Unigram dictionary
            # print(firstword, tempword)
            retWord = tokeinze_my_in_array(tempword, unigramDict, False)

            #print(retWord)
            if (retWord):
                retWord = retWord.replace('"', '')
                str += retWord + " "
            else:
                if (len(tempword) >= 1):
                    str = str + tempword + " "
        arrUnigramFiltered[key]=str
    #print(arrUnigramFiltered)
    #print(cases[3073])

    arrQuadgramFiltered = {}
    # for case in cases[startCaseNumber:endCaseNumber]:
    for key in arrUnigramFiltered:
        # print(count)
        details = arrUnigramFiltered[key]
        arrTempTerms = details.split(" ")
        lenCase = len(arrTempTerms)
        str = details;
        for i in range(lenCase):
            largestStringFound = ''
            firstword = ''
            secondword = ''
            thirdword = ''
            fourthword = ''
            firstword = arrTempTerms[i].lower()
            if (i <= (lenCase - 4)):
                secondword = arrTempTerms[i + 1].lower()
                thirdword = arrTempTerms[i + 2].lower()
                fourthword = arrTempTerms[i + 3].lower()

            if (firstword == " " or secondword == " " or thirdword == " " or fourthword == " "):
                break;
            tempword = firstword + " " + secondword + " " + thirdword + " " + fourthword
            # tempword=tempword.strim()
            # print("tempword=>",tempword)

            retword = tokenize_getNormalizedWord(tempword, ngramDict)
            if (retword):
                #print("normalized",retWord)
                str = str.replace(tempword, retword)

        arrQuadgramFiltered[key]=str

    arrTrigramFiltered = {}
    # for case in cases[startCaseNumber:endCaseNumber]:
    for key in arrQuadgramFiltered:
        # print(count)
        details = arrQuadgramFiltered[key]
        arrTempTerms = details.split(" ")
        lenCase = len(arrTempTerms)
        str = details;
        for i in range(lenCase):
            largestStringFound = ''
            firstword = ''
            secondword = ''
            thirdword = ''
            firstword = arrTempTerms[i].lower()
            if (i <= (lenCase - 3)):
                secondword = arrTempTerms[i + 1].lower()
                thirdword = arrTempTerms[i + 2].lower()

            if (firstword == " " or secondword == " " or thirdword == " "):
                break;
            tempword = firstword + " " + secondword + " " + thirdword
            # tempword=tempword.strim()
            # print("tempword=>",tempword)

            retword = tokenize_getNormalizedWord(tempword, ngramDict)
            if (retword):
                #print("normalized tri",retWord)
                str = str.replace(tempword, retword)

        arrTrigramFiltered[key]=str

    arrBigramFiltered = {}
    for key in arrTrigramFiltered:

        details = arrTrigramFiltered[key]
        arrTempTerms = details.split(" ")
        lenCase = len(arrTempTerms)
        str = ''
        # echo $details."<br/>";
        str = details;
        for i in range(lenCase):

            largestStringFound = ''
            firstword = ''
            secondword = ''
            thirdword = ''

            firstword = arrTempTerms[i].lower()
            if (i <= (lenCase - 2)):
                secondword = arrTempTerms[i + 1].lower()
            if (firstword == " " or secondword == " "):
                break
            tempword = firstword + " " + secondword
            # print("tempword=>", tempword)
            retword = tokenize_getNormalizedWord(tempword, ngramDict)
            if (retword):
                # print("normalized", retWord)
                str = str.replace(tempword, retword)

        arrBigramFiltered[key]=str

    return (cases,arrUnigramFiltered,arrQuadgramFiltered,arrTrigramFiltered,arrBigramFiltered)




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
def getIntialize(equipmentType,token):

    cases, finalizedUnigrams, finalizedQuadgrams, finalizedTrigrams, finalizedBigrams = tokenize_getNormalizedCases(equipmentType,token)
    stopwordsFile = open('stopwordsss.txt', 'r')
    stopwords = stopwordsFile.read()
    stopwordList = stopwords.split(",")

    return (cases,finalizedBigrams, stopwordList)



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

    print("N,totalWords",N,totalWords)

    if (totalWords < 10):
        print("default 10")
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
def getVectorized(equipmentType,token):

    originalCasesDict,normalizedCasesDict, stopwordList = getIntialize(equipmentType,token)
    #print(normalizedCasesDict)
    lines = []
    countToCaseIdMap = {}
    # # maximum is 4997
    count = 0
    for key in normalizedCasesDict:
        lines.append(normalizedCasesDict[key])  # now we need to vectorize the corpus
        countToCaseIdMap[count] = key
        count = count + 1


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
    #print(countDtm)
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


    return(originalCasesDict,lines,countToCaseIdMap,tfSparseMatrix,count_vect,countDtm)


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

    #print(finalList)
    '''We have got a list of list that needs to be flattened'''

    flatten = lambda l: [item for sublist in l for item in sublist]
    flattenFinalList = flatten(finalList)
    #print(flattenFinalList)

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

    return(wlJson)


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
    -cases: the dictionary of normalized cases
'''
def getWordCloudListWithCasesJson(km, N, k, casesDistributionPerCluster, countDtm, vocab,countToCaseIdMap,cases):
    wlWithCases = []
    wl = getwordList(km, N,k, casesDistributionPerCluster, countDtm, vocab)

    for key in wl:
        vocabWord = key[0]

        index = vocab.index(vocabWord)
        #print(vocabWord)
        # get all the rows for which the column at this index is nonzero
        nn = np.flatnonzero(countDtm[:, index])
        caseIds = []
        count=0
        for n in nn:
            caseIds.append({'caseId': str(countToCaseIdMap[n]),
                            'description': cases[n].replace(vocabWord, "<span class='highlight'>" + vocabWord + "</span>")})
            # caseIds.append(countToCaseIdMap[n])
            count+=1
            if(count>10):
                break
        wlWithCases.append({'tag':vocabWord,'size': str(key[1]), 'caseIds': caseIds})
    # finalResult = {}
    # finalResult["name"] = "WordCloud"
    # finalResult["errorCode"] = "Null"
    # finalResult["wordFrequencyModel"] = wlWithCases

    return (wlWithCases)




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

@app.route('/wlWithCases/<string:equipmentType>/<string:token>')
def getWordListWithcasesApi(equipmentType,token):
    #equipmentType="STEAM_TURBINE"
    #conn=''
    originalCasesDict,normalizedCases,countToCaseIdMap,tfSparseMatrix,count_vect,countDtm=getVectorized(equipmentType,token)
    km=performKmeans(tfSparseMatrix)

    #return("sjdjksdk")
    '''Find out how many documents are  in each cluster'''
    clusterDistribution = np.unique(km.labels_, return_counts=True)
    numberOfClusters=len(clusterDistribution[0])

    print(numberOfClusters)
    #return(str(numberOfClusters))

    # '''the vocabulary of the words'''
    vocab=count_vect.get_feature_names()
    #print(vocab)
    #return(printAsJSON(vocab))
    # ''' How the documents are distrbiuted in each cluster..which all cases lie in cluster 1 ..3'''
    #
    casesDistributionPerCluster=getCasesDistributionPerCluster(km)
    #print(casesDistributionPerCluster[1])
    #return(printAsJSON(casesDistributionPerCluster[1]))

    '''
    Get the word list in JSON
    '''
    #wl=getWordCloudListJson(km,len(normalizedCases),3,casesDistributionPerCluster,countDtm,vocab)
    #print(wl)
    #
    # return(wl)

    # ''''
    # Now find out how the various cases in which the cases have appeared.
    # '''
    wlWithCases=getWordCloudListWithCasesJson(km,len(normalizedCases),3,casesDistributionPerCluster,countDtm,vocab,countToCaseIdMap,normalizedCases)
    finalResult = {}
    finalResult["name"] = "WordCloud"
    finalResult["errorCode"] = "Null"
    finalResult["wordFrequencyModel"] = wlWithCases


    return(json.dumps(finalResult))

    # # dump the json to a file
    #outF = open("kmeansOutput.json", "w")
    # outF.write(json.dumps(wlWithCases))
    # outF.close()
    #



@app.route('/wl/<string:equipmentType>/<string:token>')
def getWordListApi(equipmentType,token):
    #equipmentType="STEAM_TURBINE"
    #conn=''
    originalCasesDict,normalizedCases,countToCaseIdMap,tfSparseMatrix,count_vect,countDtm=getVectorized(equipmentType,token)
    km=performKmeans(tfSparseMatrix)

    #return("sjdjksdk")
    '''Find out how many documents are  in each cluster'''
    clusterDistribution = np.unique(km.labels_, return_counts=True)
    numberOfClusters=len(clusterDistribution[0])

    #print(numberOfClusters)
    #return(str(numberOfClusters))

    # '''the vocabulary of the words'''
    vocab=count_vect.get_feature_names()
    #print(vocab)
    #return(printAsJSON(vocab))
    # ''' How the documents are distrbiuted in each cluster..which all cases lie in cluster 1 ..3'''
    #
    casesDistributionPerCluster=getCasesDistributionPerCluster(km)
    # print(casesDistributionPerCluster[1])
    #return(printAsJSON(casesDistributionPerCluster[1]))

    '''
    Get the word list in JSON
    '''
    wl=getWordCloudListJson(km,len(normalizedCases),3,casesDistributionPerCluster,countDtm,vocab)
    return(json.dumps(wl))



@app.route('/allWlWithCases/<string:token>')
def getAllWordListWithcasesApi(token):
    #equipments=['FURNACE','HRSG']

    equipments=getAllEquipmentsFromService(token)
    equipments = (json.loads(equipments))
    finalResult = {}
    finalResult["name"] = "WordCloud"
    finalResult["errorCode"] = "Null"
    #finalResult["wordFrequencyModel"] = wlWithCases
    fp = open("kmeansCasesOutput.json", "a")

    equipmentTypesJSONObjects=[]
    count=0
    for equipmentType in equipments:
        print("EQUIPMENT#.....", count, equipmentType)
        count += 1
        originalCasesDict,normalizedCases,countToCaseIdMap,tfSparseMatrix,count_vect,countDtm=getVectorized(equipmentType,token)
        km=performKmeans(tfSparseMatrix)

        '''Find out how many documents are  in each cluster'''
        clusterDistribution = np.unique(km.labels_, return_counts=True)
        numberOfClusters=len(clusterDistribution[0])

        '''the vocabulary of the words'''
        vocab=count_vect.get_feature_names()

        ''' How the documents are distrbiuted in each cluster..which all cases lie in cluster 1 ..3'''
        casesDistributionPerCluster=getCasesDistributionPerCluster(km)

        ''''
        Now find out how the various cases in which the cases have appeared.
        '''
        wlWithCases=getWordCloudListWithCasesJson(km,len(normalizedCases),3,casesDistributionPerCluster,countDtm,vocab,countToCaseIdMap,normalizedCases)

        fp.write("EquipmentType:" + equipmentType)
        fp.write(json.dumps(wlWithCases))
        equipmentTypesJSONObjects.append({'equipmentType':equipmentType,'wordFrequencyModel':wlWithCases})

    fp.close()
    finalResult = {}
    finalResult["name"] = "WordCloud"
    finalResult["errorCode"] = "Null"
    finalResult["summary"] = equipmentTypesJSONObjects

    return(json.dumps(finalResult))
        #return(wlWithCases)


@app.route('/allWl/<string:token>')
def getAllWordListApi(token):
    # equipmentType="STEAM_TURBINE"
    # conn=''
    equipments = ['FURNACE', 'HRSG']

    equipments=getAllEquipmentsFromService(token)
    equipments=(json.loads(equipments))
    finalResult = {}
    finalResult["name"] = "WordCloud"
    outF = open("kmeansOutput.json", "a")

    equipmentTypesJSONObjects = []
    count=0
    for equipmentType in equipments:
        print("EQUIPMENT#.....",count,equipmentType)
        count+=1
        originalCasesDict, normalizedCases, countToCaseIdMap, tfSparseMatrix, count_vect, countDtm = getVectorized(
            equipmentType, token)
        km = performKmeans(tfSparseMatrix)

        # return("sjdjksdk")
        '''Find out how many documents are  in each cluster'''
        clusterDistribution = np.unique(km.labels_, return_counts=True)
        numberOfClusters = len(clusterDistribution[0])

        #print(numberOfClusters)
        # return(str(numberOfClusters))

        # '''the vocabulary of the words'''
        vocab = count_vect.get_feature_names()
        #print(vocab)
        # return(printAsJSON(vocab))
        # ''' How the documents are distrbiuted in each cluster..which all cases lie in cluster 1 ..3'''
        #
        casesDistributionPerCluster = getCasesDistributionPerCluster(km)
        #print(casesDistributionPerCluster[1])
        # return(printAsJSON(casesDistributionPerCluster[1]))

        '''
        Get the word list in JSON
        '''
        wl = getWordCloudListJson(km, len(normalizedCases), 3, casesDistributionPerCluster, countDtm, vocab)
        outF.write("EquipmentType:"+equipmentType)
        outF.write(json.dumps(wl))

        equipmentTypesJSONObjects.append({'equipmentType': equipmentType, 'wordList': wl})

    outF.close()

    finalResult["data"]=equipmentTypesJSONObjects

    return(json.dumps(finalResult))


#variable accessible by the threads
equipmentTypesJSONObjectsForThreads=[]
def threadTask_old(equipmentType,token):
    print("Thread for: "+equipmentType)
    originalCasesDict, normalizedCases, countToCaseIdMap, tfSparseMatrix, count_vect, countDtm = getVectorized(
        equipmentType, token)
    km = performKmeans(tfSparseMatrix)

    '''Find out how many documents are  in each cluster'''
    clusterDistribution = np.unique(km.labels_, return_counts=True)
    numberOfClusters = len(clusterDistribution[0])

    '''the vocabulary of the words'''
    vocab = count_vect.get_feature_names()

    ''' How the documents are distrbiuted in each cluster..which all cases lie in cluster 1 ..3'''
    casesDistributionPerCluster = getCasesDistributionPerCluster(km)

    ''''
    Now find out how the various cases in which the cases have appeared.
    '''
    wlWithCases = getWordCloudListWithCasesJson(km, len(normalizedCases), 3, casesDistributionPerCluster, countDtm,
                                                vocab, countToCaseIdMap, normalizedCases)
    equipmentTypesJSONObjectsForThreads.append({'equipmentType':equipmentType,'wordFrequencyModel':wlWithCases})

    return (True)



'''
Workhorse for doing all the taks one by one
-fetch the cases
-tokenize them
-cluster
-wordcloud
'''
def threadTask(token):
    #equipments=['FURNACE','HRSG']

    equipments=getAllEquipmentsFromService(token)
    equipments = (json.loads(equipments))
    finalResult = {}
    finalResult["name"] = "WordCloud"
    finalResult["errorCode"] = "Null"
    #finalResult["wordFrequencyModel"] = wlWithCases

    equipmentTypesJSONObjects=[]
    count=0
    for equipmentType in equipments:
        print("EQUIPMENT#.....", count, equipmentType)
        count += 1
        originalCasesDict,normalizedCases,countToCaseIdMap,tfSparseMatrix,count_vect,countDtm=getVectorized(equipmentType,token)
        km=performKmeans(tfSparseMatrix)

        '''Find out how many documents are  in each cluster'''
        clusterDistribution = np.unique(km.labels_, return_counts=True)
        numberOfClusters=len(clusterDistribution[0])

        '''the vocabulary of the words'''
        vocab=count_vect.get_feature_names()

        ''' How the documents are distrbiuted in each cluster..which all cases lie in cluster 1 ..3'''
        casesDistributionPerCluster=getCasesDistributionPerCluster(km)

        ''''
        Now find out how the various cases in which the cases have appeared.
        '''
        wlWithCases=getWordCloudListWithCasesJson(km,len(normalizedCases),3,casesDistributionPerCluster,countDtm,vocab,countToCaseIdMap,normalizedCases)
        equipmentTypesJSONObjects.append({'equipmentType':equipmentType,'wordFrequencyModel':wlWithCases})

    finalResult = {}
    finalResult["name"] = "WordCloud"
    finalResult["errorCode"] = "Null"
    finalResult["summary"] = equipmentTypesJSONObjects
    print("Done With the Threading stuff")
    return("processing Completed")
    #return(json.dumps(finalResult))
        #return(wlWithCases)








from threading import Thread

@app.route('/allCasesThreades/<string:token>')
def multithreadedGetWlWithCases(token):

    #token=request.headers.get('Authorization')

    # create a list of threads
    threads = []
    #python2.7 way of cleaning the list. in 3.3+ we can simply use clear()
    del equipmentTypesJSONObjectsForThreads[:]

    #get the number of equipments
    equipments = getAllEquipmentsFromService(token)
    equipments = (json.loads(equipments))


    for equipmentType in equipments:
        # We start one thread per url present.
        process = Thread(target=threadTask, args=[equipmentType, token])
        process.start()
        threads.append(process)

    # We now pause execution on the main thread by 'joining' all of our started threads.
    # This ensures that each has finished processing the urls.
    for process in threads:
        process.join()

    finalResult = {}
    finalResult["name"] = "WordCloud"
    finalResult["errorCode"] = "Null"
    finalResult["summary"] = equipmentTypesJSONObjectsForThreads

    return (json.dumps(finalResult))



'''
This is the main route that will spawn a worker thread and then return
'''
@app.route('/asyncCases')
def shootAndForget():
    authHeader=request.headers.get('Authorization')
    token=authHeader[len("bearer "):]
    print("this is the main thread")

    process = Thread(target=threadTask, args=[token])
    process.start()

    print("this is after the thread")
    return("success")


# def helloWorld():
#     print("sjkdhjskhd")

if __name__ == '__main__':
    #app.secret_key = 'SECRET_KEY_FOR_USING_FLASK_SESSION'
    #app.secret_key = 'A0Zr98j/3yX R~XHH!jmN]LWX/,?RT'
    #call directly will work
    # getAllWordListWithcasesApi("eyJhbGciOiJSUzI1NiIsImtpZCI6ImxlZ2FjeS10b2tlbi1rZXkiLCJ0eXAiOiJKV1QifQ.eyJqdGkiOiJkZTFhMzMxZWViMjg0OTNlOTAxOWQwNmNhYTNlNDU3OCIsInN1YiI6ImRpZ2l0YWxfaXByYy1zZXJ2aWNlX3Byb2QiLCJzY29wZSI6WyJhY3MucG9saWNpZXMucmVhZCIsInVhYS5yZXNvdXJjZSIsImFjcy5hdHRyaWJ1dGVzLnJlYWQiLCJvcGVuaWQiLCJwcmVkaXgtYWNzLnpvbmVzLmFlYmE4NWY3LTdlNjMtNGMyMS1hZDBjLWExYmJkMGIyNWQ0MS51c2VyIl0sImNsaWVudF9pZCI6ImRpZ2l0YWxfaXByYy1zZXJ2aWNlX3Byb2QiLCJjaWQiOiJkaWdpdGFsX2lwcmMtc2VydmljZV9wcm9kIiwiYXpwIjoiZGlnaXRhbF9pcHJjLXNlcnZpY2VfcHJvZCIsImdyYW50X3R5cGUiOiJjbGllbnRfY3JlZGVudGlhbHMiLCJyZXZfc2lnIjoiMWZmYjY4MjkiLCJpYXQiOjE0ODQ5MDE3NjEsImV4cCI6MTQ4NDkwODk2MCwiaXNzIjoiaHR0cHM6Ly9hOTliN2ZlZS1hNDk1LTQxNjEtODljMy1mYWE4MzA1NDYyN2QucHJlZGl4LXVhYS5ydW4uYXN2LXByLmljZS5wcmVkaXguaW8vb2F1dGgvdG9rZW4iLCJ6aWQiOiJhOTliN2ZlZS1hNDk1LTQxNjEtODljMy1mYWE4MzA1NDYyN2QiLCJhdWQiOlsiYWNzLmF0dHJpYnV0ZXMiLCJ1YWEiLCJwcmVkaXgtYWNzLnpvbmVzLmFlYmE4NWY3LTdlNjMtNGMyMS1hZDBjLWExYmJkMGIyNWQ0MSIsImRpZ2l0YWxfaXByYy1zZXJ2aWNlX3Byb2QiLCJvcGVuaWQiLCJhY3MucG9saWNpZXMiXX0.d6IHoLEFFLQ2GyneHOrfyrAC-EHTYynoYEMF584w5Y2kD19-oJzPc3bhzh_q-U8vBSeMvN_BdSA6h6Ut8QcVGzqJgKr2HVZUJlkIsSXsVk41UUDrEfJpDqbvqJjD9QCv_Mp8CvQmS73WkRotrjFcVHVUYDCLWriz3GUIo6FIts9-MRZynj9K1v70mE2IIy_AT-_QnfWFQS0PYsu975cszlvyJRa054xirX5J6ohBnkgqyRBhBrnl0iJ4R2Yg9OLNq72ahvSDwt5nWTYJJIrK7VW8g1Tk-87wnO0ydJBWbKYu7BVmw-jakF7X0p6ekjsfdxFWzGw_R5BFOq-nNLYUwQ")
    app.run(host='0.0.0.0', port=port)



#create a map

