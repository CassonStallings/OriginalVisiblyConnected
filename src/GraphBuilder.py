"""
Name:       GraphBuilder.py
Purpose:    Class extending py2neo with tools to create a Neo4j graph database
            using Crunchbase data stored in MongoDB or to export Neo4j database
            for import to Gephi.
Assumes:    Data stored in MongoDB using CrunchbaseAPI.
            Local URI for Neo4j is hardcoded in class, but easily changed.
Author:     Casson Stallings, CassonStallings@gmail.com
Created:    3/12/2014
Copyright:  Casson Stallings (c) 2014
Licence:     Apache License, Version 2.0
"""

# TODO: Check that all routines are used
import copy, csv, uuid, re
from py2neo import neo4j
from py2neo import node
from pymongo import MongoClient
from py2neo.neo4j import CypherQuery


class GraphBuilder(neo4j.GraphDatabaseService):
    """Extend py2neo class to handle specifics of ETL from Mongo to Neo4j."""

    neo4j_uri = 'http://localhost:7474/db/data/'

    properties_to_delete = ['_id', 'video_embeds', 'web_presences', 'degrees', 'relationships', 'external_links',
                            'milestones', 'investments', 'image','funds', 'funding_rounds', 'providerships',
                            'tag_list', 'offices', 'partners', 'products', 'screenshots', 'competitions',
                            'acquisitions', 'acquisition', 'ipo',  'available_sizes']

    # Cypher Query
    def CypherQuery(self, cypher):
        """Returns a CypherQuery."""
        return neo4j.CypherQuery(self, cypher)

    # TODO: add acquisition and ipo to graphs

    def get_collection(self, db_name, collection_name, host='localhost', port=27017):
        """Given database and collection names returns a MongoDB collection.

        :param str db_name: name of Mongo database
        :param str collection_name: name of collection in the MongoDB
        :param str host: host for MongoDB, defaults to localhost
        :param int port: port on which Mongo is listening, defaults to 21017
        :rtype cursor: Cursor for specified collection
        """
        print 'get_collection', db_name, collection_name, host, port

        mc = MongoClient(host, port)
        return mc[db_name][collection_name]

    def add_node_collection_to_graph(self, db_name, collection_name, label, limit=0):
        """Adds all nodes in Mongo collection to Neo4j.

        :param str db_name: name of Mongo database
        :param str collection_name: name of collection in the MongoDB
        :param str label:
        :param int limit: if > 0, only limit records are added to the graph
        :rtype None:
        """
        c = self.get_collection(db_name, collection_name)
        cur = c.find(limit=limit)

        # Create a write batch to add nodes to
        batch = neo4j.WriteBatch(self)

        # Iterate over all records in the collection and add corresponding node
        for i, d in enumerate(cur):
            anode = self.get_or_add_node_to_batch(d, label_index=label, batch=batch, stub='False')
            if (i % 100) == 0:
                batch.submit()
                batch = neo4j.WriteBatch(self)
                print 'add_node_collection_to_graph', label, i, self.order
        batch.submit()
        #return anode

    def get_permalink(self, adict):
        """Given a node or edge dictionary, tries to get or build the permalink, if not returns uuid."""
        if 'permalink' in adict:
            permalink = adict['permalink']
        elif 'crunchbase_url' in adict:
            permalink = adict['crunchbase_url'].split('/', -1)[-1]
        else:
            permalink = str(uuid.uuid1())
        return permalink

    def get_or_add_node_to_batch(self, node_dict, label_index, batch, stub='', create=True):
        """Given a node dictionary, gets an existing node, or creates a new one and returns it.

        Batch node cannot be used for creating relationships in batch (use create=True).
        Assumes node has not been generated based on full attributes, it is a stub,
          so visited is set to False.

        :param dict node_dict: dictionary of properties
        :param str label_index: label for node and index used
        :param BatchObject batch: BatchObject being constructed
        :param str stub: 'True' if being created without full properties (i.e., as part of a relationship),
                        otherwise 'False'
        :param bool create: if true the node is created immediately, otherwise it is added to the batch
                if the node already exists this is ignored
        :rtype Node, BatchObject, or None: a node or BatchObject representing a node
        """
        key = 'permalink'
        value = self.get_permalink(node_dict)
        if value is None:
            return None

        # Attempt to get the node from Neo4j
        anode = self.get_indexed_node(label_index, key, value)

        # If the node was found then update the properties
        if anode:
            node_dict = self.cleanse_properties(node_dict)
            anode.update_properties(node_dict)

        # Node did not already exist, create it or add it to the batch
        if not anode:
            node_properties = self.cleanse_properties(node_dict)
            if create:
                node_properties.update({'visited': 'False', 'stub': stub})
                anode = self.get_or_create_indexed_node(label_index, key, value, node_properties)
                anode.add_labels(label_index)
            else:
                anode = batch.get_or_create_in_index(neo4j.Node, label_index, key, node(node_properties))
                batch.add_labels(anode, label_index)
                batch.set_properties(anode, {'visited': 'False', 'stub': stub})
        return anode

    def cleanse_properties(self, adict):
        """Given a dictionary, strips lists and nulls, cleans some chars, returns properties dict for node creation.

        Strips non-unicode characters from overview, name, and description.
        Removes <p> and some non-printing characters.

        :param dict adict: properties dictionary, probably from Mongo with list attributes.
        :rtype dict: dictionary with selected properties removed.
        """
        new_dict = copy.copy(adict)
        for key in self.properties_to_delete:
            if new_dict.has_key(key):
                del new_dict[key]
        for key in new_dict.keys():
            if new_dict[key] is None:
                del new_dict[key]
        new_dict = self.encode_chars(new_dict)
        return new_dict

    def encode_chars(self, adict):
        for key in ['name', 'first_name', 'last_name', 'overview', 'description',
                    'address1', 'address2', 'source_description']:
            if key in adict:
                astr = re.sub('[\t\n\r\f\v]|</?p>', ' ', adict[key], count=20)
                adict[key] = unicode(astr)
        return adict


    def add_edges_to_graph(self, db, collection, index='funder', edge_names=[], limit=0):
        """ Adds edges described in Mongo Collection to graph.

        Adds all edges of the type in collection unless limit is set.
        Assumes that funder nodes have previously been created.
        :param str db: Mongo database
        :param str collection: Collection in the database
        :param str index: index for funder nodes (funder or person)
        :param int limit: maximum records to retrieve from Mongo, if 0 all are retrieved
        :rtype None:
        """
        # Define how often to print status messages
        node_status_freq = {'funder':100,'person':2000, 'milestones':100, 'company':100}
        edge_names_by_node_type = {'funder': ['investments'],
                                   'person': ['investments', 'relationships'],
                                   'company': ['investments']
                                   }

        c = self.get_collection(db, collection)
        cur = c.find(limit=limit)
        cur.batch_size(20)
        batch = neo4j.WriteBatch(self)
        print 'Number of Nodes to Investigate ', db, collection, ':', cur.count()

        # Iterate over node collection
        for i, d in enumerate(cur):
            if 'permalink' not in d:
                continue
            if (i % node_status_freq[index]) == 0:
                batch.submit()
                neo4j.WriteBatch(self)
                print 'Adding', index, 'relationships, next iter: ', index, i
            ########item_list = d[index]
            print 'adding {} relationships from {}.'.format(index, d['permalink'])

            # Get the source node, if it doesn't exist then create one using current properties
            source_node = self.get_indexed_node(index, 'permalink', d['permalink'])
            if not source_node:
                source_properties = self.cleanse_properties(d)
                source_node = self.get_or_create_indexed_node(index, 'permalink', d['permalink'],
                                                              properties=source_properties)
            for edge_type in edge_names_by_node_type[index]:
                if not d.has_key(edge_type) or len(d[edge_type]) == 0:
                    continue
                edge_list = d[edge_type]
                for edge in edge_list:
                    if edge_type == 'investments':
                        self.add_funding_round_to_graph(source_node, edge['funding_round'], batch)
                    elif edge_type == 'relationships':
                        self.add_relationships_to_graph(source_node, edge, batch)

        batch.submit()

    def add_relationships_to_graph(self, source_node, prop_dict, batch):
        """Add relationships between people and companies/other.

        :param Node source_node: source node for relationship
        :param dict prop_dict: single dictionary from source's list
        :param WriteBatch batch: neo4j WriteBatch
        :rtype None:
        """

        if prop_dict.has_key('firm'):
            del prop_dict['firm']['type_of_entity']
            target_node = self.get_or_add_node_to_batch(prop_dict['firm'], 'company', batch=batch, stub='True',
                                                        create=True)
            del prop_dict['firm']
        elif prop_dict.has_key('person'):
            target_node = self.get_or_add_node_to_batch(prop_dict['person'], 'person', batch=batch, stub='True',
                                                        create=True)
            del prop_dict['person']

        prop_dict['current'] = True
        if prop_dict['is_past'] == 'true':
            prop_dict['current'] = False


        founder = re.compile('founder', re.I)
        ceo = re.compile('CEO|chief exec|president', re.I)
        co_vp = re.compile('C.O|vp|director|vice president|partner|chief', re.I)
        adviser = re.compile('adviser|board|consultant')

        title_string = prop_dict['title']
        if founder.search(title_string):
            title = 'Founder'
        elif ceo.search(title_string):
            title = 'CEO'
        elif co_vp.search(title_string):
            title = 'VP'
        elif adviser.search(title_string):
            title = 'Adviser'
        else:
            title = None
            print title_string
        if title and source_node and target_node:
            path = batch.get_or_create_path(source_node, (title, prop_dict), target_node)

        return None

    def add_funding_round_to_graph(self, funder_node, investment_dict, batch, relationship_type='funded'):
        """Add funding round relationships for one investor (financial org or person).

        Assumes a company is receiving the funds.

        :param Node funder_node: source node for relationship
        :param dict investment_dict: single dictionary from investor's investment list
        :param WriteBatch batch: neo4j WriterBatch
        :param str relationship_type: Type for created relationship (e.g. funded)
        """
        company_dict = investment_dict['company']

        # Add date string and delete unneeded information
        datestr = self.date_from_dictionary(investment_dict, 'funded')
        investment_dict['funded_date'] = datestr
        company_node = self.get_or_add_node_to_batch(company_dict, 'company', batch=batch, create=True)
        if company_node:
            del investment_dict['funded_month']
            del investment_dict['funded_day']
            del investment_dict['company']          # leave d with properties for relationship

            path = batch.get_or_create_path(funder_node, (relationship_type, investment_dict), company_node)
        return None

    def date_from_dictionary(self, d, prefix):
        """Derive a date string from dictionary entries for day, month, and year, return string.

        :param dict d: dictionary
        :param str prefix: string that prefixes year, month, and day variables
        :rtype str: date string yyyy-mm-dd"""
        datestr = str(d[prefix+'_year']) + '-' + str(d[prefix+'_month']) + '-' + str(d[prefix+'_day'])
        return datestr

    # def start_transaction(self):
    #     """Create Cypher transaction and return it."""
    #     print 'create transaction'
    #     session = cypher.Session("http://localhost:7474")
    #     transcript = session.create_transaction()
    #     #transcript.append(' using periodic commits 500; ')
    #     print transcript
    #     return transcript

    # def commit_transaction(self, transcript):
    #     """Commits a transaction that has been build up."""
    #     transcript.execute()
    #     transcript.commit()
    #     print 'transaction is', transcript.finished

    # def add_relationships(self, n, transcript, dict_list, relationship_type):
    #     nidx = self.get_or_create_index(neo4j.Node, 'person')
    #     #ridx = self.get_or_create_index(neo4j.Relationship, relationship_type)
    #
    #     for d in dict_list:
    #         drelation = copy.copy(d)
    #         del drelation['person']
    #
    #         transcript.append('match ' + n , '-[:' + relationship_type + {'permalink': d['person']['permalink']})
    #         n2 = self.get_or_create_indexed_node(nidx, 'permalink', d['person']['permalink'])
    #         #print 'n2', type(n2), n2
    #         if 'visited' not in n2:
    #             n2.update_properties(d['person'])
    #             n2.update_properties({'visited': False})
    #
    #         path = n.get_or_create_path((relationship_type, drelation), n2)
    #     return None

    # def add_list(self, label, dict_list, props):
    #     """Given a index specifier and list of simple items add them to db."""
    #     cnt = 0
    #     for dict in dict_list:
    #         item = dict['permalink']
    #         anode = self.create({'permakey': item})[0]
    #         anode.add_labels(label)
    #         if props:
    #             anode.update_properties(props)
    #         cnt += 1
    #     return cnt

    # def get_node_index(self, index_name):
    #     return self.get_or_create_index(neo4j.Node, index_name)

    # def get_relationship_index(self, index_name):
    #     return self.get_or_create_index(neo4j.Relationship, index_name)

    def export_person_nodes_to_csv(self, out_file_name='person_nodes.tab', limit=9999999):
        """Export person nodes to csv file to be read in with Gephi.

        Abstraction layer for export_nodes_to_csv."""
        node_type = 'person'
        query_str = 'match (n:' + node_type + ') ' + ' return n'
        initial_dict = {'label': node_type}
        person_fields = [u'nodes', u'id', u'label', u'first_name', u'last_name', u'affiliation_name', u'alias_list',
                        u'crunchbase_url',u'born_year']
        # Probably don't need: u'created_at', u'updated_at', u'twitter_username', u'blog_feed_url',
        #      u'blog_url', u'born_month', u'homepage_url', u'born_day',
        result = CypherQuery(self, 'match (n:' + node_type + ') ' + ' return count(n);').execute()
        count, = result.data[0].values
        count = min(count, limit)
        print '\nBeginning export of {} person nodes'.format(count)
        self.export_nodes_to_csv(node_type, query_str, count, out_file_name, person_fields, initial_dict, sep='\n')

    def export_company_node_to_csv(self, out_file_name='company_nodes.tab', limit=9999999):
        """Export company nodes to csv file to be read in with Gephi.

        Abstraction layer for export_nodes_to_csv."""
        node_type = 'company'
        query_str = 'match (n:' + node_type + ') ' + ' return n '
        initial_dict = {'label': node_type}
        company_fields = [u'nodes', u'id', u'label', u'name', u'category_code', u'crunchbase_url', u'description',
                           u'number_of_employees', u'alias_list', u'deadpooled_year',
                           u'total_money_raised', u'error', u'founded_year']
        # Probably don't need:  u'deadpooled_month',u'created_at', u'updated_at', u'founded_day',
        #    u'deadpooled_day', u'deadpooled_url', u'twitter_username', u'homepage_url',
        #    u'blog_url', u'blog_feed_url', u'founded_month', u'email_address',
        result = CypherQuery(self, 'match (n:' + node_type + ') ' + ' return count(n);').execute()
        count, = result.data[0].values
        count = min(count, limit)
        print '\nBeginning export of {} company nodes'.format(count)
        self.export_nodes_to_csv('company', query_str, count, out_file_name, company_fields, initial_dict, sep='\n')

    def export_financial_nodes_to_csv(self, out_file_name='financial_nodes.tab', limit=9999999):
        """Export financial institution nodes to csv file to be read in with Gephi.

        Abstraction layer for export_nodes_to_csv."""
        node_type = 'funder'
        query_str = 'match (n:' + node_type + ') ' + ' return n '
        initial_dict = {'label': node_type}
        funder_fields = [u'nodes', u'id', u'label', u'name', u'permalink', u'crunchbase_url', u'homepage_url',
                         u'description', u'overview', u'twitter_username', u'founded_year',
                         u'alias_list', u'tag_list', u'deadpooled_month',
                         u'deadpooled_year', u'total_money_raised', u'error']
        # Don't need:  u'blog_url', u'blog_feed_url', u'phone_number', u'email_address', u'founded_month',
        #   u'created_at', u'updated_at', u'founded_day', u'deadpooled_day', u'deadpooled_url',
        result = CypherQuery(self, 'match (n:' + node_type + ') ' + ' return count(n);').execute()
        count, = result.data[0].values
        count = min(count, limit)
        print '\nBeginning export of {} financial-institution nodes'.format(count)
        self.export_nodes_to_csv(node_type, query_str, count, out_file_name, funder_fields, initial_dict, sep='\n')

    def export_funded_relationships_to_csv(self, out_file_name='funded_relations.tab', limit=9999999):
        """Export edges to csv file to be read in with Gephi.

        Abstraction layer for export_relationships_to_csv."""
        rel_type = 'funded'
        query_str = 'match (a)-[r:' + rel_type + ']->(b) ' + ' return a.permalink as source, r, b.permalink as target, id(r) as id'
        initial_dict = {'label': rel_type, 'source_id': ''}
        funded_fields = [u'source', u'target', u'type', u'source_id', u'id', u'label', u'name', u'category_code',
                         u'crunchbase_url',  u'round_code', u'raised_amount',
                         u'permalink', u'source_url', u'raised_currency_code', u'funded_year']
        # Source descriptions has lots of non-standard characters--u'source_description',
        # Probably don't need: u'funded_month', u'funded_day'
        result = CypherQuery(self, 'match ()-[r:' + rel_type + ']->() ' + ' return count(r);').execute()
        count, = result.data[0].values
        count = min(count, limit)
        print 'Count rels', count
        self.export_relations_to_csv('funded', query_str, count, out_file_name, funded_fields, initial_dict, sep='\n')


    def export_relations_to_csv(self, type, query_str, count, out_file, fields, initial_dict={}, sep=','):
        """Export general relationships to csv for import to Gephi.

        :param str type: person, funder, or company
        :param str query_str: cypher query string to return the nodes or relations
        :param str out_file: output file
        :param list fields: fields to write
        :param dict initial_dict: dict with any vars not in node
        :param str sep: separator to use in output file
        :rtype None:
        """

        field_set = set()
        header_set = set()
        n_exported = 0
        n_errors = 0
        query_size = 2000

        with open(out_file, 'wb') as fil:
            dw = csv.DictWriter(fil, fields, extrasaction='ignore', dialect='excel-tab')

            # Generate and write the header line
            header = dict()
            for txt in fields:
                header[txt] = txt
                header_set.add(txt)
            dw.writerow(header)

            for first in xrange(0, count, query_size):
                query_str_with_limits = query_str + ' skip ' + str(first) + ' limit ' + str(query_size) + ';'


                query = CypherQuery(self, query_str_with_limits)
                for relationship in query.stream():

                    #print 'try to pull out rels parts', len(relationship)
                    rel_parts = relationship.values[1]
                    d = initial_dict
                    d['label'] = rel_parts.type
                    d['type'] = rel_parts.type
                    d['source'] = self.encode_chars(rel_parts.start_node)
                    d['target'] = self.encode_chars(rel_parts.end_node)
                    d['permalink'] = rel_parts.start_node['permalink'] + '__' + rel_parts.end_node['permalink']
                    edge_properties = self.encode_chars(rel_parts.get_properties())
                    d['source']['overview'] = ''
                    d['target']['overview'] = ''
                    print 'Edge_props (cleaned)', edge_properties
                    print '  Source props (cleaned)', d['permalink']
                    #print '  Source', d['source']
                   # TODO: unicode errors are in source or target node information

                    try:
                        d['id'] = d['permalink']
                        for key in d:
                            field_set.add(key)
                        d.update(edge_properties)

                        dw.writerow(self.encode_chars(d))
                        n_exported += 1
                        if (n_exported % 1000) == 0:
                            print 'Relationships exported: ', n_exported
                    except UnicodeEncodeError as uee:
                        n_errors += 1
                        print 'Unicode Error Inside in Export Relationships', uee.args
                    except ValueError as err:
                        n_errors += 1
                        print 'Unknown Error Inside in Export Relationships', err.args

        print '\nExport of {} {} relationships complete. There were {} errors.'.format(n_exported, type, n_errors)
        print '   Unexported fields: {}'.format(field_set - header_set)

    def export_nodes_to_csv(self, type, query_str, count, out_file, fields, initial_dict={}, sep=','):
        """Export general nodes to csv for import to Gephi.

        :param str type: person, funder, or company
        :param str query_str: cypher query string to return the nodes or relations
        :param str out_file: output file
        :param list fields: fields to write
        :param dict initial_dict: dict with any vars not in node
        :param str sep: separator to use in output file
        :rtype None:
        """
        field_set = set()
        header_set = set()
        n_exported = 0
        n_errors = 0
        query_size = 5000

        with open(out_file, 'wb') as fil:
            dw = csv.DictWriter(fil, fields, extrasaction='ignore', dialect='excel-tab')

            # Generate and write the header line
            header = dict()
            for txt in fields:
                header[txt] = txt
                header_set.add(txt)
            dw.writerow(header)

            for first in xrange(0, count, query_size):
                query_str_with_limits = query_str + ' skip ' + str(first) + ' limit ' + str(query_size) + ';'

                query = CypherQuery(self, query_str_with_limits)
                for item in query.stream():
                    for anode in item:
                        anode = item.values[0]
                        try:
                            d = copy.copy(initial_dict)
                            anode['permalink'] = self.get_permalink(anode)
                            d['nodes'] = anode['permalink']
                            d['id'] = anode._id
                            nd = dict()

                            for key, val in anode.get_properties().iteritems():
                                field_set.add(key)
                                nd[key] = val
                            d.update(nd)
                            dw.writerow(d)

                            n_exported += 1
                            if (n_exported % 1000) == 0:
                                print 'Nodes exported ', n_exported
                        except UnicodeEncodeError as uee:
                            n_errors += 1
                            print 'Unicode Error Inside on Nodes', uee.args
                        except ValueError as ve:
                            n_errors += 1
                            print 'Value Error Inside on Nodes', ve.args

        print '\nExport of {} {} nodes complete. There were {} errors.'.format(n_exported, type, n_errors)
        print '   Unexported fields: {}'.format(field_set - header_set)

