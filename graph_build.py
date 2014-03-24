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


crunch = None

def main():
    ##global crunch
    ###crunch = crunchbase.Crunchbase('tnfphz6pw3j9hxg2reqyzuy2')
    g = Graph('http://localhost:7474/db/data/')
    #g.clear()
    #print(g.neo4j_version)

    # Get people cursor from mongo
    idxs = g.get_indexes(neo4j.Node)

    # Add basic nodes
    #g.add_collection_to_graph('financial', 'articles', 'funder', limit=0)
    #g.add_collection_to_graph('people', 'articles', 'person', limit=0)
    #g.add_node_collection_to_graph('companies', 'articles', 'company', limit=0)

    # Add funding rounds by financial orgs and individuals
    #g.add_all_funding_rounds_to_graph('financial', 'articles', index='funder', relationship_type='funded', limit=0)
    g.add_all_funding_rounds_to_graph('people', 'articles', index='person', relationship_type='funded', limit=0)

    print 'Nodes in graph', g.order


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
    properties_to_delete = ['_id', 'video_embeds', 'web_presences', 'degrees', 'relationships', 'external_links', \
                            'milestones', 'investments', 'image','funds', 'funding_rounds', 'providerships', \
                            'tag_list', 'offices', 'partners', 'products', 'screenshots', 'competitions', \
                            'acquisitions', 'acquisition', 'ipo']

    def add_node_collection_to_graph(self, db, collection, label, limit=0):
        c = get_collection(db, collection)
        cur = c.find(limit=limit)
        batch = neo4j.WriteBatch(self)
        for i, d in enumerate(cur):
            anode = self.get_or_add_node_to_batch(d, label, batch)
            if (i % 100) == 0:
                batch.submit()
                batch = neo4j.WriteBatch(self)
                print label, i, self.order
        ##({'permalink': d['permalink']})
        batch.submit()
        #return anode

    def get_or_add_node_to_batch(self, node_dict, label_index, batch, create=True):
        """
        Given a node dictionary gets an existing node, or creates one and returns it.

        Batch node cannot be used for creating relationships in batch (use create=True).
        Assumes dictionary is stub/visited is set to False.

        node_dict: dictionary of properties
        label_index: label for node and index used
        create: if true the node is created immediately, otherwise it is added to the batch
        returns: a node or BatchObject representing a node
        """
        key = 'permalink'
        value = node_dict[key]
        anode = self.get_indexed_node(label_index, key, value)
        # If node did not already exist, create it or add it to the batch
        if not anode:
            node_properties = self.get_node_properties_from_dictionary(node_dict)
            if create:
                anode = self.get_or_create_indexed_node(label_index, key, value, node_dict)
                anode.add_labels(label_index)
                anode.update_properties({'visited': False, 'stub': True})
            else:
                anode = batch.get_or_create_in_index(neo4j.Node, label_index, key, node(node_properties))
                batch.add_labels(anode, label_index)
                batch.set_properties(anode, {'visited': False, 'stub': True})
        return anode

    def get_node_properties_from_dictionary(self, node_dict):
        """Given a dictionary, strips lists and nulls, returns properties dict for node creation.

        node_dict: properties dictionary, probably from Mongo with list attributes.
        returns: dict
        """
        node_properties = copy.copy(node_dict)
        for key in self.properties_to_delete:
            if node_properties.has_key(key):
                del node_properties[key]
        for key in node_properties.keys():
            if node_properties[key] == None:
                del node_properties[key]
        return node_properties

    def add_all_funding_rounds_to_graph(self, db, collection, index='funder', relationship_type='funded', limit=0):
        """
        Assumes that funder nodes have been created.
        db: Mongo database
        collection: Collection in the database
        index: index for funder nodes (funder or person)
        relationship_type: type of relationship being added
        limit: maximum records to retrieve from Mongo, if 0 all are retrieved
        """
        c = get_collection(db, collection)
        cur = c.find(limit=limit)
        cur.batch_size(20)
        batch = neo4j.WriteBatch(self)
        print 'Number of Funders to Traverse from ', db, collection, ':', cur.count()
        for i, d in enumerate(cur):
            if ((i % 100) == 0 and index == 'funder') or ((i % 2000) == 0 and index == 'person'):
                batch.submit()
                neo4j.WriteBatch(self)
                print 'Adding funding round relationships, starting on iter: ', i
            if not d.has_key('investments') or len(d['investments']) == 0:
                continue
            investments = d['investments']
            print 'adding investments from', index, d['permalink']
            # Get the funder node, if it doesn't exist then create one using current properties
            funder_node = self.get_indexed_node(index, 'permalink', d['permalink'])
            if not funder_node:
                node_properties = self.get_node_properties_from_dictionary(d)
                funder_node = self.get_or_create_indexed_node(index, 'permalink', d['permalink'], \
                                                              properties=node_properties)

           #funder_node = self.get_or_add_node_to_batch(d, 'funder', batch)
            self.add_funding_rounds_to_graph(funder_node, investments, batch)
        batch.submit()

    def add_funding_rounds_to_graph(self, funder_node, investments_list, batch, relationship_type='funded'):
        """Add funding round relationships for one investor (financial org or person).

        Assumes a company is receiving the funds.
        """
        for dfr in investments_list:
            d = dfr['funding_round']
            company_dict = d['company']
            del d['company']            # this leaves d with properties for relationship
            company_node = self.get_or_add_node_to_batch(company_dict, 'company', batch, create=True)
#########
            #print 'adding ', funder_node, relationship_type, company_node
            path = batch.get_or_create_path(funder_node, (relationship_type, d), company_node)
        return None


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


    def add_relationships(self, n, transcript, dict_list, relationship_type):
        nidx = self.get_or_create_index(neo4j.Node, 'person')
        #ridx = self.get_or_create_index(neo4j.Relationship, relationship_type)

        for d in dict_list:
            drelation = copy.copy(d)
            del drelation['person']

            transcript.append('match ' + n , '-[:' + relationship_type + {'permalink': d['person']['permalink']})
            n2 = self.get_or_create_indexed_node(nidx, 'permalink', d['person']['permalink'])
            #print 'n2', type(n2), n2
            if 'visited' not in n2:
                n2.update_properties(d['person'])
                n2.update_properties({'visited': False})

            path = n.get_or_create_path((relationship_type, drelation), n2)
        return None



    def get_or_create_node(self, transcript, node_label, key_val_dict):
        """
        Creates a node using key_value_dict if it does not exist.

        node_label: label (or type) for the node
        key_val_dict: property dictionary to search for/add

        Returns: a single node
        """
        astr = ' merge (n:' + node_label + str(key_val_dict) + ' ) return (n); '
        transcript.append(astr)
        return transcript


    #        if d['products']:
    #        if d['competitors']

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

    #d = crunch.get_entity('a-hack', type='financial-organization')
    #print 'result:', d

if __name__ == '__main__':
    main()
