from flask import Flask
import os
import pandas as pd
import scipy as sp
import numpy as np
import re
import csv
import json
# import psycopg2
import pickle
from sklearn.cluster import KMeans
app = Flask(__name__)

port = int(os.getenv("PORT", 64781))


#for some reason the user has to be postgres and not root
'''Db connection'''
# conn ='' psycopg2.connect(database='jim', user='postgres', password='root', host='127.0.0.1', port='5432')
conn=''

'''
Function Name:getIntiialize
Purpose: Reading teh normalized cases based on teh equipment type
Variables:
    -conn: the connection object
    -equipmentType: The category of cases

'''
@app.route('/')
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

    return(json.dumps(lines))







if __name__ == '__main__':
    app.run(host='0.0.0.0', port=port)
