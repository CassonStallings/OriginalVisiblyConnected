__author__ = 'Casson'

import requests
import pandas as pd
import matplotlib.pyplot as plt
from pymongo import MongoClient
from pymongo import errors
import time

client = MongoClient()
db = client.CB_companies

# get mongoDB collection
collection = db.people

list_of_companies = list(pdComp.permalink)

error_prod = []

def api_call(comp, api_key, q):
   a = 'http://api.crunchbase.com/v/1/company/' + comp + '.js?api_key=' + api_key
   temp = requests.get(a)
   try:
       collection.insert(temp.json())
       q.put("success for %s" % comp)
   except:
       error_prod.append(comp)
       q.put('error for %s' % comp)
#     finally:
#         sleep(1)

start = time.time()

from Queue import Queue
from threading import Thread
from time import sleep

q = Queue()
num_threads = 10
#num_folds = 3
threads = []

# Spawn a thread for each fold
for i,comp in enumerate(list_of_companies):
   i=i+1
   if len(threads) >= num_threads:
       while q.empty():
           sleep(1)
           end = time.time()
           print "queue empty @ :", end-start
       status = q.get()
   t = Thread(target=api_call, args=(comp, api_key, q))
   t.deamon = True
   t.start()
   threads.append(t)
#    end = time.time()
#    diff = end-start
#    print diff
   end = time.time()
   #Wait for all threads to complete
   for t in threads:
       t.join()