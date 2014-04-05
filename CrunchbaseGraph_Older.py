#-------------------------------------------------------------------------------
# Most of this was pulled out and put in graph_build which doesn't mix in the crunchbase
#
# Name:        crunchbase_graph_class.py
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

import crunchbase

crunch = None

def main():
    global crunch
    crunch = crunchbase.CrunchbaseGraph('tnfphz6pw3j9hxg2reqyzuy2')
    g = Graph('http://localhost:7474/db/data/')
    #print(g.neo4j_version)

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

    #transcript = g.start_transaction()
    #transcript = g.get_or_create_node(transcript, 'trash', '{ key: "trash"}')

    #cnt = add_financial_from_list(g, transcript, max_nodes=1)  ##, max_nodes=2)
    #g.commit_transaction(transcript)
    print 'done: count nodes retrieved',# cnt



class CrunchbaseGraph(neo4j.GraphDatabaseService):
    """Extend py2neo class"""

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

    def add_relationships(self, source_node, transcript, dict_list, relationship_type):
        #nidx = self.get_or_create_index(neo4j.Node, 'person')
        #ridx = self.get_or_create_index(neo4j.Relationship, relationship_type)

        for d in dict_list:
            edge_properties = copy.copy(d)
            del edge_properties['person']

            transcript.append('match ' + n , '-[:' + relationship_type + {'permalink': d['person']['permalink']})
            n2 = self.get_or_create_indexed_node('person', 'permalink', d['person']['permalink'])
            #print 'n2', type(n2), n2
            if 'visited' not in n2:
                n2.update_properties(d['person'])
                n2.update_properties({'visited': 'False', 'stub': 'True', 'created': 'add relationships: created'})

            path = source_node.get_or_create_path((relationship_type, edge_properties), n2)
        return None

    def add_investments(self, n, dict_list, relationship_type='funded'):
        #nidx = self.get_or_create_index(neo4j.Node, 'company')
        #ridx = self.get_or_create_index(neo4j.Relationship, relationship_type)
        path_list = list()
        for dfr in dict_list:
            d = dfr['funding_round']
            company_dict = d['company']
            del d['company']
            #n2 = self.get_or_create_indexed_node(nidx, 'permalink', d['permalink'])
            #print 'add_investments d', d, company_dict
            n2, = self.get_or_create_node(n, relationship_type, {'permalink': company_dict['permalink']})
            n2.add_labels('company')
            if 'visited' not in n2:
                n2.update_properties({'visited': False})
            path = n.get_or_create_path((relationship_type, d), n2)
            path_list.append(path)
        return path_list

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

    def put_in_routine_to_fold(self):
        pass

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

def add_funders_node(self, d, transcript, label='funder'):

    props = {'permalink': d['permalink']}
    print 'add_funders_node', props
    print 'original json', d
    transcript = self.get_or_create_node(transcript, label, props)

    if d['relationships']:
        # Use search pattern to define original funder node
        self.add_relationships('(n:funder '+str(props), d['relationships'], relationship_type='unknown')
        del d['relationships']
    if d['investments']:
        self.add_investments('(n:funder '+str(props), d['investments'])
        del d['investments']
    #if d['milestones']:
    #    self.add_milestones(n, d)
    #if d['offices']:
    #    self.add_offices(n, d)
    return None

if __name__ == '__main__':
    main()
