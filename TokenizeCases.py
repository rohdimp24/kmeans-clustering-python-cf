'''
This code will interface with the postgres database to read the raw cases and then write back the normalized cases to the db
the user need to specify which equipment typ cases are getting normalize.

'''

from flask import Flask
import os


app = Flask(__name__)
port = int(os.getenv("PORT", 64783))


import csv
import re
import json



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

'''
Function Name:tokenize_getCasesFromSource
Purpose: This is used to get the cases from the KBasS service. The KBaaS service will return a json. The code will parse
the JSON to fetch specific sections. for now it is just the symptoms or the case description
Variables:
    -equipmentType: which kind of equipment wind turbine, steam turbine etc

'''
def tokenize_getCasesFromSource(equipmentType):
    # cur = conn.cursor()
    # cur.execute("Select id,description from cases.smartsignal_jim_allfields where \"equipmentType\"=%s",
    #             (equipmentType,))
    #
    # rows = cur.fetchall()
    #
    # cases={}
    # #display the rows
    # for row in rows:
    #     #print (row[0],row[1])
    #     #cases[row[0]]={"original":row[1]}
    #     cases[row[0]]=row[1]
    #
    # return(cases)
    # token="eyJhbGciOiJSUzI1NiIsImtpZCI6ImxlZ2FjeS10b2tlbi1rZXkiLCJ0eXAiOiJKV1QifQ.eyJqdGkiOiIxYzFjYjRlMzM1ZWI0OWExYmQxMDM5MDRhNmEyMjBhMCIsInN1YiI6IjQwNzc1MmYwLTc4ZTktNGQyOC05YTJkLTM2NGUxMjQxMmE3MSIsInNjb3BlIjpbInBhc3N3b3JkLndyaXRlIiwib3BlbmlkIl0sImNsaWVudF9pZCI6ImluZ2VzdG9yLmVhMWEwMjQ1LTE3NTgtNGU2Ni1hYmYwLWYyNTM5NTZhNzgyMC5mOGRhYWE1MC0wYThmLTRlNWUtYWYyYS05MjYxMjA0MDRiYTEiLCJjaWQiOiJpbmdlc3Rvci5lYTFhMDI0NS0xNzU4LTRlNjYtYWJmMC1mMjUzOTU2YTc4MjAuZjhkYWFhNTAtMGE4Zi00ZTVlLWFmMmEtOTI2MTIwNDA0YmExIiwiYXpwIjoiaW5nZXN0b3IuZWExYTAyNDUtMTc1OC00ZTY2LWFiZjAtZjI1Mzk1NmE3ODIwLmY4ZGFhYTUwLTBhOGYtNGU1ZS1hZjJhLTkyNjEyMDQwNGJhMSIsImdyYW50X3R5cGUiOiJwYXNzd29yZCIsInVzZXJfaWQiOiI0MDc3NTJmMC03OGU5LTRkMjgtOWEyZC0zNjRlMTI0MTJhNzEiLCJvcmlnaW4iOiJ1YWEiLCJ1c2VyX25hbWUiOiJmNzQzYjdlZi00MmRmLTRkN2UtODlkZC05MGRjM2I1M2IwYWNfaW5nZXN0b3IiLCJlbWFpbCI6ImY3NDNiN2VmLTQyZGYtNGQ3ZS04OWRkLTkwZGMzYjUzYjBhY19pbmdlc3RvckBzdHVmLXJjLnJ1bi5hc3YtcHIuaWNlLnByZWRpeC5pbyIsImF1dGhfdGltZSI6MTQ4NDY2Njg0MCwicmV2X3NpZyI6IjgzM2U3MzFkIiwiaWF0IjoxNDg0NjY2ODQwLCJleHAiOjE0ODQ3NTMyNDAsImlzcyI6Imh0dHBzOi8vNDEyNmIyN2ItNjg2MC00OGVlLTlkYzEtOWNiYTMxM2VhYzlmLnByZWRpeC11YWEucnVuLmFzdi1wci5pY2UucHJlZGl4LmlvL29hdXRoL3Rva2VuIiwiemlkIjoiNDEyNmIyN2ItNjg2MC00OGVlLTlkYzEtOWNiYTMxM2VhYzlmIiwiYXVkIjpbInBhc3N3b3JkIiwiaW5nZXN0b3IuZWExYTAyNDUtMTc1OC00ZTY2LWFiZjAtZjI1Mzk1NmE3ODIwLmY4ZGFhYTUwLTBhOGYtNGU1ZS1hZjJhLTkyNjEyMDQwNGJhMSIsIm9wZW5pZCJdfQ.uwooVkzJcME-6Do_dlfXBLIoWWkvSoQAnhkoh8Z4SDRTg5USlp2S391QKaXVdP-D_2Huwxy0wgszYPRxysnvlAKylkRGpYhPHql5BFkj2TURfY2wIM5X7XPXv28frptOWz0o0CTyIUZc8p1TrJcqRWIUPYTClEBRlDTXLy9B5HmjpvHLHIcqExsoyw3IU_ET6l1C-isTg5p8tywHqtfHarBDTcuBJ2N5KYPlMPLSQnAmMi-RhbbYRIqeOM0YfBZ0Sor1p1BkA8fJ789OrJ4nVQh598e83MNkYaka_neDV--NSdg6gy66RxzkXGsYbFTHTeA_X-CuBqpclhjCmectqQ"
    # url = 'http://3.204.61.22:8091/v1/cases/kmbase/filteredentities?type=cases&equipmentType=COMBUSTION_TURBINE'
    # # url='https://text-mining-212470820-02.run.asv-pr.ice.predix.io/processData/30?Tenant=PredixForum&Token=Token'
    #
    # headers = {'content-type': "application/json",
    #            'Authorization': "bearer " + token,
    #            'tenant': "f743b7ef-42df-4d7e-89dd-90dc3b53b0ac"
    #            }
    #
    # resp = requests.get(url, headers=headers)
    #
    # print(resp)
    # return (resp.text)

    '''Currently reading from a  local json..this json will be fetched from a service finally based on the equipement type'''
    import json
    cases={}
    with open('/Users/305015992/pythonProjects/kmeans-python/jsondata.json') as json_data:
        firstLevel = json.load(json_data)
        # print(type(tt))
        for firstLevelKeys in firstLevel:
            caseId = firstLevelKeys['caseId']
            # json.loads will convert the string to dictionary so that we can access using a key
            secondLevel = json.loads(firstLevelKeys['data'])
            print("----")
            print(caseId, secondLevel['symptoms'])
            cases[caseId]=secondLevel['symptoms']
    return(cases)



'''
Function Name:tokenize_getCasesAndDictionaryFromSource
Purpose: This is sused to fetch teh cases and the dictionary from teh source. The dictionary has been seperately genertaed
Variables:
    -equipmentType: which kind of equipment wind turbine, steam turbine etc

'''
def tokenize_getCasesAndDictionaryFromSource(equipmentType):
	# fname = "all_jim_case_large.txt"
	# with open(fname, 'r') as myfile:
	# 	data = myfile.read()
    #
	# data = data.split('-------BREAK--------')
	# cases = [case.strip() for case in data]
	# #print(cases[1:10])
    cases=tokenize_getCasesFromSource(equipmentType)
    dictFile = "/Users/305015992/pythonProjects/wordcloud/dict.csv"
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

def tokenize_getNormalizedCases(equipmentType):

    cases,unigramDict,ngramDict=tokenize_getCasesAndDictionaryFromSource(equipmentType)

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
        case = case.replace('~', ' ')
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


def testdata():
    return("rohit")

# def printcases(caseId,finalizedUnigrams,finalizedQuadgrams,finalizedTrigrams,finalizedBigrams):
#     print(cases[caseId])
#     print(finalizedUnigrams[caseId])
#     print(finalizedQuadgrams[caseId])
#     print(finalizedTrigrams[caseId])
#     print(finalizedBigrams[caseId])



''''Main code that will insert the tokenized cases to db'''

'''
the various equipment types are
1.FURNACE
2.HRSG
3.GEARBOX
4.CONDENSER
5.MILL
6.WIND_TURBINE
7.FEEDWATER_HEATER
8.SUBMERSIBLE_PUMP
9.RECIPROCATING_ENGINE
10.MOTOR
11.BOILER_FEED_PUMP
12.CHILLER
13.HOT_GAS_EXPANDER
14.GENERATOR
15.BLOWER
16.COMPRESSOR
17.LNG
18.HEAT_EXCHANGER
19.LN
20.AIR_HEATER
21.NONE
22.PUMP
23.STEAM_TURBINE
24.COOLING_TOWER
25.COMBUSTION_TURBINE
26.UNDEFINED





'''

@app.route('/')
def getNormalizedCasesList():

    equipmentType="STEAM_TURBINE"


    print(equipmentType)

    #return(tokenize_getCases(conn,equipmentType))
    cases,finalizedUnigrams,finalizedQuadgrams,finalizedTrigrams,finalizedBigrams=tokenize_getNormalizedCases(equipmentType)
    return(str(len(finalizedBigrams)))

    print(cases[15952])
    print(finalizedBigrams[15952])
    for key in finalizedBigrams:
        print(key,finalizedBigrams[key])
    #return(len(cases),len(finalizedUnigrams),len(finalizedQuadgrams),len(finalizedTrigrams),len(finalizedBigrams))

    # caseId=24
    #
    # printcases(caseId)
    # cur = conn.cursor()
    # count=0
    # for key in cases:
    #     print(cases[key])
    #     print(finalizedBigrams[key])
    #     count+=1
    #     if(count>10):
    #         break
    #     # query = "INSERT INTO cases.smartsignal_normalized_case(id, \"originalCase\", \"normalizedCase\",\"equipmentType\") VALUES (%s, %s, %s,%s);"
    #     # data = (key, cases[key], finalizedBigrams[key],equipmentType)
    #     # cur.execute(query, data)
    #
    # conn.commit()
    #



if __name__ == '__main__':
    app.run(host='0.0.0.0', port=port)


