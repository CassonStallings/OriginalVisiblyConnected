
"""
Name:        crunchbase_to_mongo.py (was crunchbase2.py)
Purpose:     Pulls data from crunchbase through the API and stores JSON
               documents in MongoDB which may be running on AWS. It
               set up to get all data, not data related to specific
               queries.
               Contains class CrunchbaseApi
Requires:    CrunchbaseApi API key and if used AWS ID and key pair.
Author:      Casson
Created:     3/12/2014
Copyright:   (c) Casson 2014
Licence:     <your licence>

Program is typically run from command line and given parameters
indicating which links to follow.

:param int startid:  first item in entity list to fetch, defaults to 0
:param int count:  number of items to fetch, defaults to 0, fetch all
:param int num_of_list: entity list to use
    1. financial-organizations
    2. people
    3. companies
    4. products
    5. service-providers
:param str log_file: file to store output
"""

import sys
from code.CrunchbaseApi import CrunchbaseApi
from pymongo import MongoClient

start_id = 1
count = 5
num_of_list = 2
log_file_name = "process_log.txt"
pickle_dir = 'c:/users/casson/desktop/startups/data/'

if len(sys.argv)>=2:
    start_id = int(sys.argv[1])
if len(sys.argv)>=3:
    count = int(sys.argv[2])
if len(sys.argv)>=4:
    num_of_list = int(sys.argv[3])
if len(sys.argv)>=5:
    log_file_name = sys.argv[4]

print 'Running with start_id:', start_id,'  count:', count, '  num_of_list:', num_of_list, '  log_file:', log_file_name

entity_type_tuples = [('financial-organizations', 'financial-organization'), ('people', 'person'),
                      ('companies', 'company'), ('products', 'product'),
                      ('service-providers', 'service-provider')]

def main():
    print 'Starting Main'
    open_log_file = open(log_file_name, 'w')
    crunch = CrunchbaseApi(open_log_file = open_log_file)
    client = MongoClient()
    mc = client.crunchbase


    # Get pertinent lists to drive downloads
    # These are saved in S3/casson_lists
    #crunch.put_entity_list_in_s3(list_type='financial_organizations')
    #crunch.put_entity_list_in_s3(list_type='people')
    #crunch.put_entity_list_in_s3(list_type='companies')


    #for entity_type_list, entity_type in entity_type_tuples:
    #    crunch.mongo_collection = mc[entity_type_list]
    #    entity_list = crunch.get_entity_list_and_pickle(entity_type_list, entity_type + '_list.pkl')

    for entity_type_list, entity_type in [entity_type_tuples[num_of_list]]:
        mongo_collection = mc[entity_type_list.replace('-', '_')]
        entity_list = crunch.get_pickled_entity_list(pickle_dir + entity_type + '_list.pkl')
        crunch.cycle_through_permalinks(entity_type, entity_list[start_id:start_id+count], mongo_collection)

    open_log_file.close()
    print 'Done Cycling Through Permalinks'

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


if __name__ == '__main__':
    main()
