#-------------------------------------------------------------------------------
# Name:        crunchbase.py Befor changes for boto
# Purpose:
# Author:      Casson
# Created:     12/03/2014
# Copyright:   (c) Casson 2014
# Licence:     <your licence>
#-------------------------------------------------------------------------------

import requests
import json
import time

sleep_time = 0.0
api = 'tnfphz6pw3j9hxg2reqyzuy2'


def main():
    pass


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
        """Initialize a Crunchbase object.

        :param api_key:
        """
        self.base = 'http://api.crunchbase.com/v/1/'
        assert isinstance(api_key, object)
        self.api_key = api_key

    def get_entity(self, permakey, entity_type='company'):
        """Gets a single entity type and returns a dictionary. """
        #print 'get_entity', permakey, type
        params = {'api_key': api}  ###, 'Entity': type, 'Name': permalink}
        url = self.base + entity_type + '/' + permakey + '.js'
        #print 'url', url, params
        #wait for a bit so we are not hitting it too fast..
        time.sleep(sleep_time)
        try:
            r = requests.get(url, params=params)
            if r.status_code >= 400:
                print '\nERROR {} while retrieving page {}'.format(r.status_code, url)
                return None
        except:
            print 'Waiting 5 seconds before continuing.', url, params
            time.sleep(5)
            return None

        #print 'get entity, requests status ',r.status_code
        try:
            result = json.loads(r.content)
        except:
            print '\nERROR in get_entity loading json\n', r.content
            result = None

        return result

    def get_entity_list(self, entity_type):
        """Gets complete list of entity type and returns a list of dictionaries."""
        url = self.base + entity_type + '.js'
        assert isinstance(self.api_key, object)
        params = {'api_key': self.api_key}
        r = requests.get(url, params=params)

        if r.status_code >= 400:
            print '\nERROR {} while retrieving page {}'.format(r.status_code, url)
            return None
        return json.loads(r.content)

    def get_financial_list(self):
        """Gets list of all financial organizations returns a list of dicts."""
        return self.get_entity_list('financial-organizations')

    def get_company_list(self):
        """Gets list of all companies returns a list of dicts."""
        return self.get_entity_list('companies')

    def get_person_list(self):
        """Gets list of all people and returns a list of dicts."""
        return self.get_entity_list('people')


if __name__ == '__main__':
    main()
