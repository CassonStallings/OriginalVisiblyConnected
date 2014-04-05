#-------------------------------------------------------------------------------
# Name:        graph_build.py
# Purpose:     extends py2neo Graph
# Author:      Casson
# Created:     12/03/2014
# Copyright:   (c) Casson 2014
# Licence:     <your licence>
#-------------------------------------------------------------------------------
#import logging
#logging.basicConfig(level=logging.WARNING)
import logging
import copy
from py2neo import neo4j
from py2neo import cypher
from py2neo import node, rel
from pymongo import MongoClient
import re
import cProfile
import networkx as nx
import cPickle


crunch = None

def main():
    ##global crunch
    ###crunch = crunchbase.Crunchbase('tnfphz6pw3j9hxg2reqyzuy2')
    g = Graph('http://localhost:7474/db/data/')
    #g.clear()
    #print(g.neo4j_version)
    #g.delete_index(neo4j.Node, 'funders')


    nl1 = g.create_node_collection('crunchbase', 'people', 'person', stub='False', skip=0, limit=100000)
    print 'nl1'
    nl2 = g.create_node_collection('crunchbase', 'financial_organizations', 'funder', stub='False', skip=0, limit=100000)
    print 'nl2'
    nl3 = g.create_node_collection('crunchbase', 'companies', 'company', stub='False', skip=0, limit=100000)
    print 'nl3'

    e1 = g.create_edge_collection('crunchbase', 'financial_organizations', index='funder', skip=0, limit=100000)
    print 'e1'
    e2 = g.create_edge_collection('crunchbase', 'people', index='person', skip=0, limit=100000)
    print 'e2'

    print 'nl1', len(nl1), nl1[1]
    print 'e1', len(e1), e1[1]
    print 'e2', len(e2), e2[1]
    gnx = nx.DiGraph()
    gnx.add_nodes_from(nl1)
    gnx.add_nodes_from(nl2)
    gnx.add_nodes_from(nl3)
    gnx.add_edges_from(e1)
    gnx.add_edges_from(e2)

    print 'num nodes prior to removal', nx.number_of_nodes(gnx)
    gnx.remove_nodes_from(nx.isolates(gnx))
    print 'num nodes after removal', nx.number_of_nodes(gnx)

    with open(r'C:\Users\Casson\Desktop\Startups\Data\graph_nodes.pkl', 'w') as fil:
        cPickle.dump(gnx,fil)

    print 'num nodes', nx.number_of_nodes(gnx)
    print 'num edges', nx.number_of_edges(gnx)

    # Get people cursor from mongo
    #idxs = g.get_ind# exes(neo4j.Node)
    #print idxs
    #g.add_edges_to_graph('crunchbase', 'financial_organizations', index='funder', relationship_type='funded', skip=0, limit=0)
    #g.add_edges_to_graph('crunchbase', 'people', index='person', relationship_type='funded', skip=4650, limit=0)

    # # Add basic nodes
    #g.add_node_collection_to_graph('crunchbase', 'financial_organizations', 'funder', limit=0)
    #g.add_node_collection_to_graph('crunchbase', 'people', 'person', limit=0)
    #g.add_node_collection_to_graph('crunchbase', 'companies', 'company', limit=0)
    # #
    # # # Add funding rounds by financial orgs and individuals

    #print 'Nodes and relationships in graph', g.order, g.size


    # Get the list of financial organizations and put in db as nodes
    # Got 14417 financial companies/entities
    #dict_list = crunch.get_financial_list()
    #cnt = g.add_list('funders_to_get', dict_list, props={'visited': False})
    #print 'count of financial orgs', cnt

    # Get list of companies and put in db as nodes with label companies_to_get
    # Got about 223,000 companies
    #dict_list = crunch.get_company_list()
    #print 'Dict List is ', dict_list
    #cnt = g.add_list('companies_to_get', dict_list)
    #print 'Count of companies', cnt
# end graph transaction
#    transcript = g.start_transaction()
    #transcript = g.get_or_create_node(transcript, 'trash', '{ key: "trash"}')

    #cnt = add_financial_from_list(g, transcript, max_nodes=1)  ##, max_nodes=2)
# commit graph transactions
#    g.commit_transaction(transcript)
    #rint 'done: count nodes retrieved',# cnt

def get_collection(db, collection='articles', host='localhost', port=27017):
    """Given database and collection names returns a collection."""
    mc = MongoClient(host, port)
    collection = mc[db][collection]
    return collection

class Graph(neo4j.GraphDatabaseService):
    """Extend py2neo class"""

    properties_to_use = ['funding_rounds']
    properties_to_delete = ['_id', 'video_embeds', 'web_presences', 'degrees', 'relationships', 'external_links',
                            'milestones', 'investments', 'image','funds', 'funding_rounds', 'providerships',
                            'tag_list', 'offices', 'partners', 'products', 'screenshots', 'competitions',
                            'acquisitions', 'acquisition', 'ipo']

    def create_edge_collection(self, db, collection, index='funder', skip=0, limit=0):
        """
        Assumes that funder nodes have been created.
        db: Mongo database
        collection: Collection in the database
        index: index for funder nodes (funder or person)
        relationship_type: type of relationship being added
        limit: maximum records to retrieve from Mongo, if 0 all are retrieved
        """
        # Define how often to print status messages
        node_status_freq = {'funder':100,'person':50, 'milestones':100}
        edge_names_by_node_type = {'funder': ['investments'],
                                   'person': ['investments', 'relationships'],
                                   'company': []
                                   }
        new_edge_list = list()

        c = get_collection(db, collection)
        cur = c.find(limit=limit)
        cur.batch_size(100)
        batch = neo4j.WriteBatch(self)
        print 'Number of Nodes to Investigate ', db, collection, ':', cur.count()

        # Iterate over node collection
        for i, d in enumerate(cur):
            if i < skip:
                continue
            key = 'permalink'
            source_id = self.get_permalink(d, key)
            if not source_id:
                continue
            d[key] = source_id

            # Get the source node, if it doesn't exist then create one using current properties
            #source_node = self.get_indexed_node(index, key, value)
            #if not source_node:
            #    source_properties = self.get_node_properties_from_dictionary(d)
            #    source_node = self.get_or_create_indexed_node(index, key, value, properties=source_properties)
            for edge_type in edge_names_by_node_type[index]:

                if not d.has_key(edge_type) or d[edge_type] is None or len(d[edge_type]) == 0:
                    continue
                #print 'edge type', edge_type
                edge_list = d[edge_type]
                for edge in edge_list:
                    if edge_type == 'investments':
                        new_edge = self.create_funding_round(source_id, edge['funding_round'])
                    elif edge_type == 'relationships':
                        new_edge = self.create_relationships(source_id, edge)
                    if new_edge:
                        new_edge_list.append(new_edge)
        return new_edge_list

    def create_relationships(self, source_id, edge_dict, relationship_type=''):
        """For networkx
        Add funding round relationships for one investor (financial org or person).

        Assumes a company is receiving the funds.
        self: graph object
        source_node: source node for relationship
        prop_dict: single dictionary from source's list
        batch: neo4j WriterBatch
        relationship_type: Type for created relationship-varies for relationships #####
        """
        #print 'add_relationships to graph'
        key = 'permalink'
        # value = self.get_permalink(edge_dict, key)
        # if not value:
        #     return None
        # edge_dict[key] = value

        if edge_dict.has_key('firm'):
            target_id = self.get_permalink(edge_dict['firm'])
            del edge_dict['firm']
        elif edge_dict.has_key('person'):
            target_id = self.get_permalink(edge_dict['person'])
            del edge_dict['person']

        edge_dict['current'] = 'True'
        if edge_dict['is_past'] == 'true':
            edge_dict['current'] = 'False'

        founder = re.compile('founder|founding|owner|principal', re.I)
        ceo = re.compile('CEO|chief exec|president', re.I)
        co_vp = re.compile('C.O|vp|director|vice president|partner|chief', re.I)
        manager = re.compile('manager|operations|operating', re.I)
        adviser = re.compile('adviser|board|consultant', re.I)
        investor = re.compile('investor', re.I)

        title_string = edge_dict['title']
        if founder.search(title_string):
            title = 'Founder'
            edge_dict['weight'] = 10
            edge_dict['distance'] = 1
        elif ceo.search(title_string):
            title = 'CEO'
            edge_dict['weight'] = 10
            edge_dict['distance'] = 1
        elif co_vp.search(title_string):
            title = 'VP'
            edge_dict['weight'] = 10
            edge_dict['distance'] = 1
        elif investor.search(title_string):
            title = 'Investor'
            edge_dict['weight'] = 8
            edge_dict['distance'] = 2
        elif adviser.search(title_string):
            title = 'Adviser'
            edge_dict['weight'] = 7
            edge_dict['distance'] = 3
        elif manager.search(title_string):
            title = 'Manager'
            edge_dict['weight'] = 6
            edge_dict['distance'] = 4
        else:
            title = 'Other'
            edge_dict['weight'] = 2
            edge_dict['distance'] = 8
            #print 'Unspecified title: ', title_string
        if title:
            edge_dict['title'] = title

        return (source_id, target_id, edge_dict)

    def create_funding_round(self, funder_id, investment_dict, relationship_type='funded'):
        """
        Add funding round relationships for one investor (financial org or person).

        Assumes a company is receiving the funds.
        self: graph object
        funder_node: source node for relationship
        investment_dict: single dictionary from investor's investment list
        batch: neo4j WriterBatch
        relationship_type: Type for created relationship
        """
        company_dict = investment_dict['company']
        key = 'permalink'
        target_id = self.get_permalink(company_dict)

        # Add date string and delete unneeded information
        datestr = self.date_from_dictionary(investment_dict, 'funded')
        investment_dict['funded_date'] = datestr
        investment_dict['weight'] = 10
        investment_dict['distance'] = 1

        if not target_id:
            return None
        if investment_dict.has_key('funded_month'):
            del investment_dict['funded_month']
        if investment_dict.has_key('funded_day'):
            del investment_dict['funded_day']
        del investment_dict['company']          # leave d with properties for relationship

        return (funder_id, target_id, investment_dict)

    def create_node_collection(self, db, collection, label, stub='False', skip=0, limit=0):
        c = get_collection(db, collection)
        cur = c.find(limit=limit)
        #batch = neo4j.WriteBatch(self)
        #fil = open(file_name, 'w')
        node_list = list()
        for i, d in enumerate(cur):

            if i < skip :
                continue

            key = 'permalink'
            d[key] = self.get_permalink(d)
            value = d[key]

            d = self.get_node_properties_from_dictionary(d)
            d['stub'] = str(stub)
            if label == 'company':
                #print 'company'
                self.clean_company_node(d)
            node_list.append((value, d))

        return node_list

    def add_node_collection_to_neo4j_graph(self, db, collection, label, limit=0):
        c = get_collection(db, collection)
        cur = c.find(limit=limit)
        batch = neo4j.WriteBatch(self)
        for i, d in enumerate(cur):

            if i < 0 : ##34600:  ## Through people 64000 and 67000 to 68600 companies through 40000 except last 200 of each 5000
                continue
            # Process nodes if they are stubs
            if d.has_key('stub') and d['stub'] == 'True':
                if d.has_key('image') and len(d['image']) > 0:
                    d['picture'] = 'crunchbase.com/' + d['image']['available_sizes'][0][2]
                    print 'picture', d['picture']
                if d.has_key('offices') and len(d['offices']) > 0:
                    off_dict = d['offices']
                    d['country'] = off_dict['country_code']
                    d['state'] = off_dict['state_code']
                    d['city'] = off_dict['city_code']
                    print 'country, state', d['country'], d['state']

            anode = self.get_or_add_node_to_batch(d, label_index=label, batch=batch, stub='False')
            if not anode:
                continue
            if (i % 200) == 0:
                batch.submit()
                batch = neo4j.WriteBatch(self)
                print 'add_node_collection_to_graph', label, i, self.order
        ##({'permalink': d['permalink']})
        batch.submit()
        #return anode

    def rrrrrrrrrrrrrrrrrrrrrrrrrrrrcreate_node(self, node_dict, label_index, file, stub='False'):
        """
        Given a node dictionary gets an existing node, or creates one and returns it.

        Batch node cannot be used for creating relationships in batch (use create=True).
        Assumes dictionary is stub/visited is set to False.

        node_dict: dictionary of properties
        label_index: label for node and index used
        stub: True if being created without full properties (i.e., as part of a relationship)
        create: if true the node is created immediately, otherwise it is added to the batch.
                if the node already exists this is ignored.
        returns: a node or BatchObject representing a node
        """
        key = 'permalink'
        node_dict[key] = self.get_permalink(node_dict)
        value = node_dict[key]
        #print label_index, key, value
        anode = self.get_indexed_node(label_index, key, value)
        if anode:
            #print '\nabout to update', node_dict
            #print type(node_dict), anode
            node_dict = self.get_node_properties_from_dictionary(node_dict)
            anode.update_properties(node_dict)
            anode.update_properties({'stub': str(stub)})
            if label_index == 'company':
                #print 'company'
                anode = self.clean_company_node(anode)

        # If node did not already exist, create it or add it to the batch
        else:
            node_properties = self.get_node_properties_from_dictionary(node_dict)
            #print node_properties
            if create:
                #print 'creating:', key, value
                anode = self.get_or_create_indexed_node(label_index, key, value, node_properties)
                anode.add_labels(label_index)
                anode.update_properties({'visited': 'False', 'stub': str(stub)})
                anode.update_properties({'created': 'get or add node to batch, created immediately '})
            else:
                node_properties['created'] = 'get or add node to batch, added to batch '
                anode = batch.get_or_create_in_index(neo4j.Node, label_index, key, value, node(node_properties))
                batch.add_labels(anode, label_index)
                batch.set_properties(anode, {'visited': 'False', 'stub': str(stub)})
        return anode

    def get_or_add_node_to_batch(self, node_dict, label_index, batch, stub='True', create=True):
        """
        Given a node dictionary gets an existing node, or creates one and returns it.

        Batch node cannot be used for creating relationships in batch (use create=True).
        Assumes dictionary is stub/visited is set to False.

        node_dict: dictionary of properties
        label_index: label for node and index used
        stub: True if being created without full properties (i.e., as part of a relationship)
        create: if true the node is created immediately, otherwise it is added to the batch.
                if the node already exists this is ignored.
        returns: a node or BatchObject representing a node
        """
        key = 'permalink'
        node_dict[key] = self.get_permalink(node_dict)
        value = node_dict[key]
        #print label_index, key, value
        anode = self.get_indexed_node(label_index, key, value)
        if anode:
            #print '\nabout to update', node_dict
            #print type(node_dict), anode
            node_dict = self.get_node_properties_from_dictionary(node_dict)
            anode.update_properties(node_dict)
            anode.update_properties({'stub': str(stub)})
            if label_index == 'company':
                #print 'company'
                anode = self.clean_company_node(anode)

        # If node did not already exist, create it or add it to the batch
        else:
            node_properties = self.get_node_properties_from_dictionary(node_dict)
            #print node_properties
            if create:
                #print 'creating:', key, value
                anode = self.get_or_create_indexed_node(label_index, key, value, node_properties)
                anode.add_labels(label_index)
                anode.update_properties({'visited': 'False', 'stub': str(stub)})
                anode.update_properties({'created': 'get or add node to batch, created immediately '})
            else:
                node_properties['created'] = 'get or add node to batch, added to batch '
                anode = batch.get_or_create_in_index(neo4j.Node, label_index, key, value, node(node_properties))
                batch.add_labels(anode, label_index)
                batch.set_properties(anode, {'visited': 'False', 'stub': str(stub)})
        return anode

    def get_node_properties_from_dictionary(self, node_dict):
        """Given a dictionary, strips lists and nulls, returns properties dict for node creation.

        node_dict: properties dictionary, probably from Mongo with list attributes.
        returns: dict
        """
        node_properties = copy.copy(node_dict)

        node_properties = self.add_image_to_node(node_properties)
        node_properties = self.add_office_to_node(node_properties)

        for key in self.properties_to_delete:
            if node_properties.has_key(key):
                del node_properties[key]
        for key in node_properties.keys():
            if node_properties[key] == None:
                del node_properties[key]
        return node_properties

    def add_image_to_node(self, node_dict):

        if node_dict.has_key('image'):
            if node_dict['image']:
               if len(node_dict['image']) > 0:
                    node_dict['picture'] = node_dict['image']['available_sizes'][0][1]
        return node_dict

    def add_office_to_node(self, node_dict):

        if node_dict.has_key('offices'):
            if node_dict['offices']:
                if len(node_dict['offices']) > 0:
                    node_dict['country'] = node_dict['offices'][0]['country_code']
                    node_dict['state'] = node_dict['offices'][0]['state_code']
                    node_dict['city'] = node_dict['offices'][0]['city']

        return node_dict

    def add_edges_to_neo4j_graph(self, db, collection, index='funder', edge_names=[], relationship_type='funded', skip=0, limit=0):
        """
        Assumes that funder nodes have been created.
        db: Mongo database
        collection: Collection in the database
        index: index for funder nodes (funder or person)
        relationship_type: type of relationship being added
        limit: maximum records to retrieve from Mongo, if 0 all are retrieved
        """
        # Define how often to print status messages
        node_status_freq = {'funder':100,'person':50, 'milestones':100}
        edge_names_by_node_type = {'funder': ['investments'],
                                   'person': ['investments', 'relationships'],
                                   'company': []
                                   }

        c = get_collection(db, collection)
        cur = c.find(limit=limit)
        cur.batch_size(50)
        batch = neo4j.WriteBatch(self)
        print 'Number of Nodes to Investigate ', db, collection, ':', cur.count()

        # Iterate over node collection
        for i, d in enumerate(cur):
            if i < skip:
                continue
            if (i % node_status_freq[index]) == 0:
                print 'submitting Edges % to %', i-i, i
                batch.submit()
                neo4j.WriteBatch(self)

            key = 'permalink'
            value = self.get_permalink(d, key)
            if not value:
                return None
            d[key] = value

            # Get the source node, if it doesn't exist then create one using current properties
            source_node = self.get_indexed_node(index, key, value)
            if not source_node:
                source_properties = self.get_node_properties_from_dictionary(d)
                source_node = self.get_or_create_indexed_node(index, key, value, properties=source_properties)
            for edge_type in edge_names_by_node_type[index]:
                #print '\nedge type', edge_type, type(edge_type)
                #print d
                #print d[edge_type]

                if not d.has_key(edge_type) or d[edge_type] is None or len(d[edge_type]) == 0:
                    continue
                #print 'edge type', edge_type
                edge_list = d[edge_type]
                for edge in edge_list:
                    if edge_type == 'investments':
                        self.add_funding_round_to_graph(source_node, edge['funding_round'], batch)
                    elif edge_type == 'relationships':
                        self.add_relationships_to_graph(source_node, edge, batch)

        if batch:
            batch.submit()

    def add_relationships_to_graph(self, source_node, edge_dict, batch, relationship_type=''):
        """
        Add funding round relationships for one investor (financial org or person).

        Assumes a company is receiving the funds.
        self: graph object
        source_node: source node for relationship
        prop_dict: single dictionary from source's list
        batch: neo4j WriterBatch
        relationship_type: Type for created relationship-varies for relationships #####
        """
        #print 'add_relationships to graph'
        key = 'permalink'
        value = self.get_permalink(edge_dict, key)
        if not value:
            return None
        edge_dict[key] = value

        if edge_dict.has_key('firm'):
            #print 'in firm'
            value = self.get_permalink(edge_dict['firm'])
        #print 'add_relationships to graph'
        key = 'permalink'
        value = self.get_permalink(edge_dict, key)
        if not value:
            return None
        edge_dict[key] = value

        if edge_dict.has_key('firm'):
            #print 'in firm'
            value = self.get_permalink(edge_dict['firm'])
            #del edge_dict['firm']['type_of_entity']
            target_node = self.get_or_add_node_to_batch(edge_dict['firm'], 'company', batch=True, stub='True', create=True)
            del edge_dict['firm']
        elif edge_dict.has_key('person'):
            #print 'in person'
            value = self.get_permalink(edge_dict['person'])
            target_node = self.get_or_add_node_to_batch(edge_dict['person'], 'person', batch=True, stub='True', create=True)
            del edge_dict['person']

        edge_dict['current'] = 'True'
        if edge_dict['is_past'] == 'true':
            edge_dict['current'] = 'False'

        founder = re.compile('founder|founding|owner|principal', re.I)
        ceo = re.compile('CEO|chief exec|president', re.I)
        co_vp = re.compile('C.O|vp|director|vice president|partner|chief', re.I)
        manager = re.compile('manager|operations|operating', re.I)
        adviser = re.compile('adviser|board|consultant', re.I)
        investor = re.compile('investor', re.I)

        title_string = edge_dict['title']
        if founder.search(title_string):
            title = 'Founder'
            edge_dict['weight'] = 10
            edge_dict['distance'] = 1
        elif ceo.search(title_string):
            title = 'CEO'
            edge_dict['weight'] = 10
            edge_dict['distance'] = 1
        elif co_vp.search(title_string):
            title = 'VP'
            edge_dict['weight'] = 10
            edge_dict['distance'] = 1
        elif investor.search(title_string):
            title = 'Investor'
            edge_dict['weight'] = 8
            edge_dict['distance'] = 2
        elif adviser.search(title_string):
            title = 'Adviser'
            edge_dict['weight'] = 7
            edge_dict['distance'] = 3
        elif manager.search(title_string):
            title = 'Manager'
            edge_dict['weight'] = 6
            edge_dict['distance'] = 4
        else:
            title = 'Other'
            edge_dict['weight'] = 2
            edge_dict['distance'] = 8
            print 'Unspecified title: ', title_string
        if title:
            edge_dict['title'] = title
            path = batch.get_or_create_path(source_node, (title, edge_dict), target_node)

        return None

    def get_permalink(self, d, key='permalink'):
        value = None
        #print 'top',key, d
        if d.has_key(key):
            value = d[key]
            #print 'sec if', value
        elif d.has_key('crunchbase_url'):
            value = d['crunchbase_url'].split(r'/')[-1]
            #print 'else', value
        if not value:
            print 'Skipping object, no permalink', d
        return value

    def add_funding_round_to_graph(self, funder_node, investment_dict, batch, relationship_type='funded'):
        """
        Add funding round relationships for one investor (financial org or person).

        Assumes a company is receiving the funds.
        self: graph object
        funder_node: source node for relationship
        investment_dict: single dictionary from investor's investment list
        batch: neo4j WriterBatch
        relationship_type: Type for created relationship
        """
        company_dict = investment_dict['company']

        # Add date string and delete unneeded information
        datestr = self.date_from_dictionary(investment_dict, 'funded')
        investment_dict['funded_date'] = datestr
        investment_dict['weight'] = 10
        investment_dict['distance'] = 1
        company_node = self.get_or_add_node_to_batch(company_dict, 'company', batch=True, stub='True', create=True)
        if not company_node:
            return None
        if investment_dict.has_key('funded_month'):
            del investment_dict['funded_month']
        if investment_dict.has_key('funded_month'):
            del investment_dict['funded_day']
        del investment_dict['company']          # leave d with properties for relationship

        path = batch.get_or_create_path(funder_node, (relationship_type, investment_dict), company_node)
        return None

    def clean_company_node(self, anode):
        if 'founded_year' in anode and 'founded_month' in anode and 'founded_day' in anode:
            anode['founded'] = self.date_from_dictionary(anode, 'founded')
            #print anode['founded']
        anode['closed'] = 'unknown'
        if 'deadpooled_year' in anode and 'deadpooled_day' in anode and anode['deadpooled_year'] > 1900:
            anode['closed'] = self.date_from_dictionary(anode, 'deadpooled')
        #print anode['founded']

    def date_from_dictionary(self, d, prefix):
        """Derive a date string from dictionary entries for day, month, and year, return string."""
        datestr = 'unknown'
        if d[prefix+'_year']:
            datestr = str(d[prefix+'_year']) + '-' + str(d[prefix+'_month']) + '-' + str(d[prefix+'_day'])
            for k in ['_year', '_month', '_day']:
                del d[prefix+k]
        return datestr

    def start_transaction(self):
        """Create Cypher transaction and return it."""
        print 'create transaction'
        session = cypher.Session("http://localhost:7474")
        transcript = session.create_transaction()
        #transcript.append(' using periodic commits 500; ')
        print transcript
        return transcript

    def commit_transaction(self, transcript):
        """Commits a transaction that has been build up."""
        transcript.execute()
        transcript.commit()
        print 'transaction is', transcript.finished

    def not_used_add_relationships(self, n, transcript, dict_list, relationship_type):
        #nidx = self.get_or_create_index(neo4j.Node, 'person')
        #ridx = self.get_or_create_index(neo4j.Relationship, relationship_type)

        for d in dict_list:
            drelation = copy.copy(d)
            del drelation['person']

            transcript.append('match ' + n , '-[:' + relationship_type + {'permalink': d['person']['permalink']})
            n2 = self.get_or_create_indexed_node('person', 'permalink', d['person']['permalink'])
            #print 'n2', type(n2), n2
            if 'visited' not in n2:
                n2.update_properties(d['person'])
                n2.update_properties({'visited': False})

            path = n.get_or_create_path((relationship_type, drelation), n2)
        return None

    def not_used_get_or_create_node(self, transcript, node_label, key_val_dict):
        """
        Creates a node using key_value_dict if it does not exist.

        node_label: label (or type) for the node
        key_val_dict: property dictionary to search for/add

        Returns: a single node
        """
        astr = ' merge (n:' + node_label + str(key_val_dict) + ' ) return (n); '
        transcript.append(astr)
        return transcript

    def add_list(self, label, dict_list, props):
        """Given a index specifier and list of simple items add them to db."""
        cnt = 0
        for dict in dict_list:
            item = dict['permalink']
            anode = self.create({'permakey': item})[0]
            anode.add_labels(label)
            if props:
                anode.update_properties(props)
            cnt += 1
            #print 'key, label, value, node:', akey, label, item, anode, len(item)
        return cnt

    def get_node_index(self, index_name):
        return self.get_or_create_index(neo4j.Node, index_name)

    def get_relationship_index(self, index_name):
        return self.get_or_create_index(neo4j.Relationship, index_name)

#---------------------------------------------------------------------------------------------------------
# Older code below this point
def add_financial_from_list(g, transcript, list_to_get='funders_to_get', label='funders', max_nodes=20000):
    global crunch

    #idx = g.get_node_index('funders')
    anode_list = g.find(list_to_get)
    #print 'length of node list', len(list(anode_list))

    cnt = 0
    anode_list = list(anode_list)
    for n in anode_list[8326:]:
        #print 'Top of loop', n['visited']
        if 'visited' in n:
            if n['visited'] is True or n['visited'] == 'true':
                n.update_properties({'visited': True})  # Make sure Trues get capitalized
                #print 'Already visited funder', n['permalink']
                continue
        cnt += 1
        #print cnt
        if (cnt % 50) == 0:
            print '\nGetting ', cnt, n['permakey']
        if max_nodes < cnt:
            return cnt
        #print 'node', n

        d = crunch.get_entity(n['permakey'], entity_type='financial-organization')
        if d:
            transcript = g.add_funders_node(d, transcript)
            n.update_properties({'visited': True})

        # Set visited to None when get entity runs into an error
        else:
            n.update_properties({'visited': None})
    return cnt

def routine_to_fold_old_code():
    pass
    #d = crunch.get_entity('a-hack', type='financial-organization')
    #print 'result:', d
    #############################


    ##########################
    # def add_person_to_batch(self, d, batch, label='person'):
    #     """
    #     d: dictionary defining person from crunchbase api
    #     batch: neo4j batch request object (for writing)
    #     label: label for node
    #     """
    #     for key in self.properties_to_delete:
    #         if d.has_key(key):
    #             del d[key]
    #     anode = batch.create(node(d))
    #     batch.add_labels(anode, 'Person')
    #     ##({'permalink': d['permalink']})
    #
    #     return anode
    #
    #
    # def add_financial_to_batch(self, d, batch, label='funder'):
    #     """
    #     d: dictionary defining person from crunchbase api
    #     batch: neo4j batch request object (for writing)
    #     label: label for node
    #     """
    #     for key in self.properties_to_delete:
    #         if d.has_key(key):
    #             del d[key]
    #     anode = batch.create(node(d))
    #     batch.add_labels(anode, label)
    #     ##({'permalink': d['permalink']})
    #


      # def add_funders_node(self, d, index='funders'):
   #      """:type self: object
   #      """
   #      #idx = self.get_or_create_index(neo4j.Node, index)
   #      n, = self.create({'permalink': d['permalink']})
   #      #n = self.get_or_create_indexed_node(idx, 'permalink', value=d['permalink'])
   #      print d['permalink']
   #      n.add_labels('funder')
   #      #self.add_funders_links(n, d)
   #      if d['relationships']:
   #          self.add_relationships(n, d['relationships'], relationship_type='unknown')
   #          del d['relationships']
   #      if d['investments']:
   #          self.add_investments(n, d['investments'])
   #          del d['investments']
   #      #if d['milestones']:
   #      #    self.add_milestones(n, d)
   #      #if d['offices']:
   #      #    self.add_offices(n, d)
   #      return n
   #
   #
   #  def add_relationships(self, n, dict_list, relationship_type):
   #      #nidx = self.get_or_create_index(neo4j.Node, 'person')
   #      #ridx = self.get_or_create_index(neo4j.Relationship, relationship_type)
   #
   #      for d in dict_list:
   #          drelation = copy.copy(d)
   #          del drelation['person']
   #
   #          n2, = self.get_or_create_node(n, relationship_type, {'permalink': d['person']['permalink']})
   #          #n2 = self.get_or_create_indexed_node(nidx, 'permalink', d['person']['permalink'])
   #          #print 'n2', type(n2), n2
   #          if 'visited' not in n2:
   #              n2.update_properties(d['person'])
   #              n2.update_properties({'visited': False})
   #
   #          path = n.get_or_create_path((relationship_type, drelation), n2)
   #      return None
   #
   #  def add_investments(self, n, dict_list, relationship_type='funded'):
   #      #nidx = self.get_or_create_index(neo4j.Node, 'company')
   #      #ridx = self.get_or_create_index(neo4j.Relationship, relationship_type)
   #      path_list = list()
   #      for dfr in dict_list:
   #          d = dfr['funding_round']
   #          company_dict = d['company']
   #          del d['company']
   #          #n2 = self.get_or_create_indexed_node(nidx, 'permalink', d['permalink'])
   #          #print 'add_investments d', d, company_dict
   #          n2, = self.get_or_create_node(n, relationship_type, {'permalink': company_dict['permalink']})
   #          n2.add_labels('company')
   #          if 'visited' not in n2:
   #              n2.update_properties({'visited': False})
   #          path = n.get_or_create_path((relationship_type, d), n2)
   #          path_list.append(path)
   #      return path_list
   #
   #  def get_or_create_node(self, n1, rel, key_value_dict):
   #      """
   #      Creates a node using key_value_dict if it does not exist.
   #
   #      n1: an existing node
   #      rel: the relationship between the nodes
   #      key_value_dict: {key: value} to search for in related nodes
   #
   #      Returns: list of a single node (for compatibility)
   #      """
   #      new_node = None
   #      key, = key_value_dict.keys()
   #      value = key_value_dict[key]
   #      node_list = self.match(n1, rel)
   #      for n in node_list:
   #          if key in n:
   #              if value == n[key]:
   #                  new_node = [n]  # Put node in list so it is consistent with create
   #                  continue
   #      if new_node is None:
   #          new_node = self.create(key_value_dict)
   #      return new_node
   #
   #
   #  #        if d['products']:
   #  #        if d['competitors']


   #################################

      # def add_funders_node(self, d, transcript, label='funder'):
    #
    #     props = {'permalink': d['permalink']}
    #     print 'add_funders_node', props
    #     print 'original json', d
    #     transcript = self.get_or_create_node(transcript, label, props)
    #     ##({'permalink': d['permalink']})
    #
    #     if d['relationships']:
    #         # Use search pattern to define original funder node
    #         self.add_relationships('(n:funder '+str(props), d['relationships'], relationship_type='unknown')
    #         del d['relationships']
    #     if d['investments']:
    #         self.add_investments('(n:funder '+str(props), d['investments'])
    #         del d['investments']
    #     #if d['milestones']:
    #     #    self.add_milestones(n, d)
    #     #if d['offices']:
    #     #    self.add_offices(n, d)
    #     return None

if __name__ == '__main__':
    #cProfile.run('main()')
    main()
