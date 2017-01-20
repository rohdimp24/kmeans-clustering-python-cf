from flask import Flask
import os
import numpy as np
import re
import json
import csv
from sklearn.cluster import KMeans
import requests
import time
#from kmeans-python import TokenizeCases

from threading import Thread

app = Flask(__name__)

port = int(os.getenv("PORT", 64782))

threadAccessibleVariable=[]
def threadTask(id):
    print("starting the new threa")

    time.sleep(10)
    print("%s: %s" % (id, time.ctime(time.time())))
    threadAccessibleVariable.append(id)
    return(True)

@app.route('/')
def checkMultiThread():
    # create a list of threads
    threads = []
    equipmentTypesJSONObjectsForThreads = []
    # get the number of equipments
    threadAccessibleVariable.clear()

    for id in range(20):
        # We start one thread per url present.
        process = Thread(target=threadTask, args=[id])
        process.start()
        threads.append(process)

    # We now pause execution on the main thread by 'joining' all of our started threads.
    # This ensures that each has finished processing the urls.
    for process in threads:
        process.join()

    return(json.dumps(threadAccessibleVariable))


@app.route('/shoot')
def checkShootAndForget():

    print("this is the main thread")

    process = Thread(target=threadTask, args=[1])
    process.start()

    print("this is after the thread")
    return("sdjskd")

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=port)

import re



cc="bearer sdshjkdsjkdhkshdk"
cc[len("bearer "):]
