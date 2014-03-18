import pymongo
import requests
import json, cPickle
import copy
import time
#from bs4 import BeautifulSoup
import re
#from pymongo import MongoClient

angel_base = 'https://api.angel.co/1/'
total_pages = 0
sleep_time = 2
proxie = {"http" : " https://187.188.195.66:8080",
        "https" : "https://187.188.195.66:8080"}

def angel_slug(slug):
    params = {'query': slug}
    url = angel_base + 'search'
    r = requests.get(url, params=params)
    return adict

def angel_search(qstring, type):
    params = {'query': qstring, 'type': type}
    url = angel_base + 'search'
    r = requests.get(url, params=params)
    return r


def get_locs_from_page(page_dict, loc_dict):
    '''Given dicts pull loc records out of page_dict and add to loc_dict.'''
    # Children (locations) are given as list of dictionaries in dict paged
    childlst = page_dict['children']
    for dchild in childlst:
        item_id = str(dchild['id'])
        if not loc_dict.has_key(item_id):
            # Remove unneeded information and add to adict
            #del dchild['id']
            dchild['all_startups'] = dchild['statistics']['all']['startups']
            dchild['direct_startups'] = dchild['statistics']['direct']['startups']
            del dchild['statistics']
            loc_dict[item_id] = dchild
    return loc_dict


def get_loc_pages(page_id, parse_func=get_locs_from_page):
    global total_pages, sleep_time, proxies
    # Get first page and page count associated with this id
    page = 1
    params = {'page': page}  #'id': page_id,
    url = angel_base + 'tags/' + str(page_id) + '/children'
    print 'Getting page {} of {}'.format(page, url)
    total_pages += 1
    r = requests.get(url, params=params, proxies=proxies)
    paged = json.loads(r.content)
    if paged.has_key('error'):
        print 'ERROR: on returned page', url, paged
        return None

    page = paged['page']            # This page number
    lastpage = paged['last_page']   # Number of pages
    total = paged['total']          # Number of entries

    loc_dict = {}
    loc_dict = parse_func(paged, loc_dict)

    for p in xrange(2, lastpage+1):
        params['page'] = p
        url =angel_base + 'tags/' + str(page_id) + '/children'
        print 'Getting page {} of {}'.format(page, url)
        total_pages += 1
        time.sleep(sleep_time)
        r = requests.get(url, params=params, proxies=proxies)
        if r.status_code != 200:
            print 'WARNING: Page not returned. Error:', r.status_code,
        else:
            paged = json.loads(r.content)
            page = paged['page']
            if page != p:
                print 'WARNING: Pages not equal: p, page', p, page
            tmp_dict = parse_func(paged, loc_dict)
            for key, d in tmp_dict.iteritems():
                if key not in loc_dict:
                    adict = {key: d}
                    loc_dict[key] = adict
    return loc_dict

def write_pages(loc_name='locations_us.pkl', remaining_name='remaining_ids.pkl', page_dict=None, id_set=None):
    fil = open(loc_name, 'wb')
    cPickle.dump(page_dict, fil)
    fil.close()

    fil = open(remaining_name, 'wb')
    adict ={"remaining_ids": list(id_set)}
    cPickle.dump(adict, fil)
    fil.close()
    return


def get_links(id_list, loc_name='locations_us.pkl', remaining_name='remaining_ids.pkl',
            parse_func=get_locs_from_page, max_pages=900):
    global total_pages
    item_dict = {}                           # In case no items are grabbed
    all_item_dict = {}
    visited = set()
    id_set = set(id_list)

    while id_set != set():
        if total_pages > max_pages:
            print 'Max pages read ({}), {} ids remaining, saving results to {}'.format(
                   max_pages, len(id_set), loc_name)
            write_pages(loc_name, remaining_name, all_item_dict, id_set)
            return all_item_dict, id_set
        page_id = id_set.pop()
        visited.add(page_id)
        if (not all_item_dict.has_key(page_id)) or (all_item_dict.has_key(page_id) and all_item_dict[page_id]['all_startups'] > 0):
            item_dict = get_loc_pages(page_id, parse_func=parse_func)
            for key, d in item_dict.iteritems():
                all_item_dict[key] = d
        # Expand list of ids that need to be visited
        new_set = set(item_dict.keys())
        new_set.difference_update(visited)  # Remove visited ids from new_set
        #print 'new_set', new_set, '\n'
        id_set.update(new_set)
        write_pages(loc_name, remaining_name, all_item_dict, id_set)
    print 'Complete: pages read ({}), no ids remaining, saving results to {}'.format(
                   total_pages, len(id_set), loc_name)
    return all_item_dict, id_set

def get_remainders(remaining_name='remaining_ids.pkl'):
    fil = open(remaining_name, 'rb')
    d = cPickle.load(fil)
    fil.close()
    return d['remaining_ids']

def combine_pages():
    all_locs = {}
    directs = 0
    alls = 0
    for n in ['0', '2', '3']:
        d0 = cPickle.load(open('locations_us'+n+'.pkl', 'rb'))
        for k in d0.iterkeys():
            if all_locs.has_key(k):
                print 'Duplicate key in '+n+': ', k
            else:
                all_locs[k] = d0[k]
                alls += d0[k]['all_startups']
                directs += d0[k]['direct_startups']
    print 'Got all locs, len:', len(all_locs)
    print 'all {}, direct {}'.format(alls, directs)
    cPickle.dump(all_locs, open('locations_us.pkl', 'wb'))
    return all_locs


#def get_oremaindersl(remaining_name='remaining_ids.pkl'):
#    fil = open(remaining_name, 'rb')
#    d = cPickle.load(fil)
#    fil.close()
#    return d['remaining_ids']

#id_list = ['1688'] # id_list = get_remainders(remaining_name='remaining_ids3.pkl')
#print id_list

#r = angel_search('B', 'LocationTag')
# North America = 1665; United States = 1688;
# Louisville = 1654; North Carolina = 1666
#proxies = {}
#all_locs, not_visited = get_links(id_list, max_pages=990, loc_name='locations_us0.pkl',
#                    remaining_name='remaining_ids0.pkl');

d = combine_pages()

#print 'Final Dictionary \n', all_locs

#alls = directs = 0
#for key,d in all_locs.iteritems():
#    directi = d['direct_startups']
#    alli = d['all_startups']
#    alls += alli
#    directs += directi
    #print 'Startups for {}: all, direct: {} {}'.format(d['name'], alli, directi)
#print 'Total all and direct', alls, directs


b='''
def search_page(search_term='zipf', section_dict={'web_url':None}, page=0):
    global key
    articles = list()
    nyt_base = "http://api.nytimes.com/svc/search/v2/articlesearch"
    request_format = '.json'
    request_dict = {'api-key':key, 'page':page, 'begin_date':'20140101'}  #'q':search_term,
    r = requests.get(nyt_base+request_format, params=request_dict)
    if not r.status_code == 200:
        print 'Bad Status Code: ', r.status_code
        return None

    jobj = json.loads(r.content)
    total_articles = jobj['response']['meta']['hits']
    doclst = jobj['response']['docs']
    for d in doclst:
        sdict = copy.copy(section_dict)
        for k in sdict.iterkeys():
            sdict[k] = d[k]
        articles.append(sdict)
    return (articles, total_articles)


def search(search_term='zipf', section_dict={'web_url':None}, first_page=0, max_pages=2):
    articles = list()
    art_lst, art_cnt = search_page(search_term, section_dict, page=0)
    page_count = int(first_page + ceil(art_cnt/10))
    for page in xrange(1, min(page_count+1, max_pages+1)):
        print 'Retrieving Search Page', page
        art_lst, art_cnt = search_page(search_term, section_dict, page=page)
        articles.extend(art_lst)
        #print 'page loop', page, len(articles), art_lst[0]
    return articles

def get_article(aurl):
    return requests.get(aurl)

def save_articles(url_list, mongo_coll):
    cnt = 0
    for a_dict in url_list:
        u = a_dict['web_url']
        #print 'url', u
        if u:
            response = get_article(u)
            full_page = response.text  #json
            #print 'fullpage', response.text
            art_soup = BeautifulSoup(response.text)
            art_text = art_soup.find(id='main')
            #id=re.compile("area-main-center-w-left|story-body-text story-content|entry-update|content")
            if art_text != None:
                cnt += 1
                print 'saving', cnt, u[0:40]
                mongo_coll.insert({'url':u, 'text':art_text.text})#, 'page':full_page})
    return None
'''
a='''
client = MongoClient()
artdb = client.artdb
artgrp = artdb.artgrp

articles = search(search_term='', first_page=101, max_pages=20)

cnt = save_articles(articles, artgrp)
print cnt
'''

