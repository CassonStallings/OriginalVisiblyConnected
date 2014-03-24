#-------------------------------------------------------------------------------
# Name:        crunchbase.py-Incorporating boto
# Purpose:
# Author:      Casson
# Created:     12/03/2014
# Copyright:   (c) Casson 2014
# Licence:     <your licence>
#-------------------------------------------------------------------------------

import requests
import json
from simplejson.decoder import JSONDecodeError
import requests
from pymongo import MongoClient
from pymongo import errors
import time
from Queue import Queue
from threading import Thread
from time import sleep
import cPickle

#import boto
#import boto.ec2
from boto.s3.key import Key
from boto.s3.connection import S3Connection

sleep_time = 0.0
sleep_time_if_problems = 5.0

#mine  crunchbase_api = 'tnfphz6pw3j9hxg2reqyzuy2'  # Crunchbase API
crunchbase_api = 'w6jcmetvsadsga6sbxefgntn'  #  Jon's Crunchbase API
aws_id = 'AKIAIDAYSH4Y27YHDFYQ'  # aws_access_key_id
aws_key = 'rXTjT64Fx8G7er8t39n+UZsqX0WgbXCBxW7kHaXc'  # aws_secret_access_key

mongo_uri = 'mongodb://localhost:27017'  #'mongodb://54.226.78.144:27017'

entity_type_tuples = [('financial-organizations', 'financial-organization'), ('people', 'person'), \
                      ('companies', 'company'), ('products', 'product'), \
                      ('service-providers', 'service-provider')]
fil = None

q = Queue()
mongo_collection = None
num_threads = 10
threads = []
q = Queue()
error_prod = list()


def main():
    global fil
    crunch = Crunchbase(crunchbase_api)

    # Get pertinent lists to drive downloads
    # These are saved in S3/casson_lists
    #crunch.put_entity_list_in_s3(list_type='financial_organizations')
    #crunch.put_entity_list_in_s3(list_type='people')
    #crunch.put_entity_list_in_s3(list_type='companies')


    fil = open('progress.txt', 'w')

    client = MongoClient(mongo_uri)
    mc = client.crunchbase

    #for entity_type_list, entity_type in entity_type_tuples:
    #    crunch.mongo_collection = mc[entity_type_list]
    #    entity_list = crunch.get_entity_list_and_pickle(entity_type_list, entity_type + '_list.pkl')


    for entity_type_list, entity_type in entity_type_tuples[1:2]:
        mongo_collection = mc[entity_type_list]
        entity_list = crunch.get_pickled_entity_list(entity_type + '_list.pkl')
        crunch.cycle_through_permalinks(entity_type, entity_list)

    fil.close()

    # entity_list = crunch.get_entity_list_and_pickle('financial-organizations', 'financial_list.pkl')
    # entity_list = crunch.get_pickled_entity_list('financial_list.pkl')
    # crunch.cycle_through_permalinks('financial-organization', entity_list)

    #OLD
    #print 'store webpages, key dict list', key_dict_lst
    #cnt = crunch.store_webpages(key_dict_lst, entity_type='financial_organization',
    #                                save_bucket='financial_organization_data_store')
    #print 'done cnt', cnt

    # Companies
    #co_key_dict_lst = crunch.get_entity_list_from_s3(entity_type='company', list_bucket='companies_list')
    #cocnt = crunch.store_webpages(co_key_dict_lst, entity_type='company', save_bucket='company_data_store')


    # People
    #pe_key_dict_lst = crunch.get_entity_list_from_s3(entity_type='person', list_bucket='people_list')
    #pecnt = crunch.store_webpages(pe_key_dict_lst, entity_type='person', save_bucket='people_data_store')


class Crunchbase():
    """
    Entities, typically identified by a type variable, include:
        financial-organization(s),
        company(ies),
        person(people),
        product(s),
        service_provider(s)
    """

    def __init__(self, api_key):
        """Initialize a Crunchbase object."""
        self.base = 'http://api.crunchbase.com/v/1/'
        self.api_key = api_key

    #    self.num_threads = 10
    #    self.threads = []
    #    self.q = Queue()
    #    self.error_prod = list()

    def put_entity_list_in_s3(self, list_type='financial_organizations'):
        """Call api for a type and get all current permalinks, store in s3."""
        global aws_id, aws_key, api_key

        conn = S3Connection(aws_id, aws_key)
        bucket = conn.create_bucket(list_type + '_list')
        k = Key(bucket)

        jlist = self.get_entity_list(list_type)
        for num, dlike in enumerate(jlist):
            d = dict(dlike)
            key = d['permalink']
            k.key = key
            k.set_contents_from_string(d)
        print 'Done putting data into', list_type, 'last d, last key', d, key


    def get_entity_list_from_s3(self, entity_type, list_bucket):
        """Get permalinks of one type from s3 and return a list."""
        global aws_id, aws_key
        print 'in get_entity_list_from _s3, type, bucket', entity_type, list_bucket
        cnt = 0
        # Get a list of entities
        conn = S3Connection(aws_id, aws_key)
        list_bucket = conn.get_bucket(list_bucket)
        key_generator = list_bucket.list()
        # print 'count in key_list', len(list(key_generator))
        permalink_list = list()
        for key in key_generator:
            try:
                dstr = key.get_contents_as_string()
                d = dict(dstr)
                permalink = d['permalink']
                cnt += 1
            except:
                continue
            permalink_list.append(permalink)

        print 'Have list of {} keys from {}. '.format(cnt, entity_type)
        return permalink_list



        #print 'temp', temp
        #try:
        #     d = temp.json() ##json.loads(temp.content)
        # except ValueError:  #ValueError:
        #     print 'Likely JSONDecode error on ', temp.status_code, ' ', url
        #     q.put('error')
        #     return
        # #t1 = time.time()
        # d['_id'] = permalink
        # try:
        #     mongo_collection.save(d, w=0)
        #     #t2 = time.time()
        # except:
        #     error_prod.append(permalink)
        #     #print '\nurl', url
        #     #print 'temp.status_code', temp.status_code
        #     #print 'temp.content', temp.content
        #     q.put('error for %s' % permalink)
        #     return
        #
        # q.put("success for %s" % permalink)
        # #q.task_done()
        # #q.task_done()
        # #print 'Call API Execution Time',time.time()-t0

    def cycle_through_permalinks(self, entity_type, permalink_list):
        global crunchbase_api,fil
        t0 = i = 0

        for i, link in enumerate(permalink_list):
            i = i + 1
            if (i % 10) == 0:
                out_str = 'Entity_type, Iter, time for last 10:' + entity_type + ' ' + str(i) + ' ' + str(time.time()-t0)
                print out_str
                fil.writelines([out_str])
                fil.flush()
                t0 = time.time()
            if len(threads) >= num_threads:
                while q.empty():
                    sleep(1)
                    end = time.time()
                    #print "queue empty @ :"   #, end - start
                status = q.get()
            t = Thread(target=api_call, args=(link, entity_type, crunchbase_api, q))
            t.deamon = True
            t.start()
            threads.append(t)
        for t in threads:
            t.join()
            #    end = time.time()
            #    diff = end-start
            #    print diff
            #end = time.time()

            #     def cycle_through_permalinks(self, entity_type, permalink_list):
            #          global fil, num_threads, q, threads
            #          # Spawn a thread for each fold
            #          t0 = time.time()
            #          for i, permalink in enumerate(permalink_list):
            #              i=i+1
            #              if i % 10 == 0:
            #                  print 'type, count', entity_type, i, ' ' + str(time.time()-t0), 'Threads:', len(threads)
            #                  fil.writelines('\ntype, count:' + entity_type + ' ' + str(i) + ' ' + str(time.time()-t0))
            #                  fil.flush()
            #                  t0 = time.time()
            #
            #              if len(threads) >= num_threads:
            #                  #print q.qsize()
            #                  #print 'Sleeping Momentarily'
            #                  while q.empty():
            #                      sleep(1.3)
            #                      #end = time.time()
            #                      #print "queue empty @ :"  #, end-start
            #                  status = q.get()
            #              #print time.time()
            #              q.put(permalink)
            #              t = Thread(target=self.make_api_call, args=(entity_type, permalink))   #, permalink))
            #              t.deamon = True
            #              t.start()
            #              threads.append(t)
            #         #    end = time.time()
            #         #    diff = end-start
            #         #    print diff
            #              end = time.time()
            #             #Wait for all threads to complete
            # #         s = q.get()

    def store_webpages(self, key_dict_lst, entity_type, save_bucket):
        """OLD---Given list of permalinks (in dicts), download web pages from crunchbase, and save in s3."""
        global aws_id, aws_key

        # Set up bucket for save
        print 'store_webpages'
        conn = S3Connection(aws_id, aws_key)
        bucket = conn.get_bucket(save_bucket)
        k = Key(bucket)

        cnt = 0
        print 'Starting loop to get and store pages:', len(key_dict_lst.keys())
        for cnt, d in enumerate(key_dict_lst):
            print 'looping in enumerate, cnt, d\n      ', cnt, d, 'key=', d['permakey']
            permakey = d['permakey']
            webpage_as_json = self.get_entity(permakey, entity_type=entity_type)
            k = permakey
            cnt += 1
            k.set_contents_from_string(webpage_as_json)
            if cnt > 5:
                break
            print 'store_webpage', cnt
        return cnt

    def get_entity(self, permakey, entity_type='company'):
        """OLD---Gets a single entity type from crunchbase and returns a dictionary."""
        params = {'api_key': crunchbase_api}  ###, 'Entity': type, 'Name': permalink}
        url = self.base + entity_type + '/' + permakey + '.js'
        time.sleep(sleep_time)  # Wait for a bit so we are not hitting it too fast..
        try:
            r = requests.get(url, params=params)
            if r.status_code >= 400:
                print '\nERROR {} while retrieving page {}'.format(r.status_code, url)
                return None
        except:
            print 'Waiting 5 seconds before continuing.', url, params
            time.sleep(sleep_time_if_problems)
            return None
        try:
            result = json.loads(r.content)
        except:
            print '\nERROR in get_entity loading json\n', r.content
            result = None

        return result

    def get_entity_list(self, entity_type):
        """Gets complete list of entity type from crunchbase and returns a list of dictionaries."""
        url = self.base + entity_type + '.js'
        params = {'api_key': self.api_key}
        r = requests.get(url, params=params)

        if r.status_code >= 400:
            print '\nERROR {} while retrieving page {}'.format(r.status_code, url)
            print '   URL attempted:', r.url
            return None
        return json.loads(r.content)

    def get_entity_list_and_pickle(self, entity_type, pickle_file):
        """Gets complete list of entity type from crunchbase and returns it as a list."""
        url = self.base + entity_type + '.js'
        params = {'api_key': self.api_key}
        r = requests.get(url, params=params)
        if r.status_code >= 400:
            print '\nERROR {} while retrieving page {}'.format(r.status_code, url)
            print '   URL attempted:', r.url
            return

        d = json.loads(r.content)
        entity_list = list()
        for adict in d:
            entity_list.append(adict['permalink'])
        cPickle.dump(entity_list, open(pickle_file, 'wb'))
        return entity_list

    def get_pickled_entity_list(self, pickle_file):
        """Gets a complete entity list from a pickle file and returns a list."""
        return cPickle.load(open(pickle_file, 'rb'))


        # def get_financial_list(self):
        #     """Gets list of all financial organizations returns a list of dicts."""
        #     return self.get_entity_list('financial-organizations')
        #
        # def get_company_list(self):
        #     """Gets list of all companies returns a list of dicts."""
        #     return self.get_entity_list('companies')
        #
        # def get_person_list(self):
        #     """Gets list of all people and returns a list of dicts."""
        #     return self.get_entity_list('people')

   # def make_api_call(self, entity_type, aaapermalink):  #, permalink):
   #      global mongo_collection, crunchbase_api, q, error_prod
   #      t0 = time.time()
   #      permalink = q.get()
   #      url = self.base + entity_type + '/' + permalink + '.js?api_key=' + crunchbase_api
   #      #print 'url', url
   #      #t0 = time.time()
   #      temp = requests.get(url)

def api_call(link, entity_type, api_key, q):
    global mongo_collection, error_prod
    a = 'http://api.crunchbase.com/v/1/' + entity_type + '/' + link + '.js?api_key=' + api_key
    temp = requests.get(a)
    try:
        d = temp.json()
        d['_id'] = d['permakey']
        mongo_collection.insert(d)
        q.put("success for %s" % link)
    except:
        error_prod.append(link)
        q.put('error for %s' % link)

#     finally:
#         sleep(1)

# Spawn a thread for each fold

if __name__ == '__main__':
    main()