# TODO: Check that all routines are used
import copy, csv, uuid, re
from py2neo import neo4j
from py2neo import cypher
from py2neo import node, rel
from pymongo import MongoClient
from py2neo.neo4j import CypherQuery


class GraphBuilder(neo4j.GraphDatabaseService):
    """Extend py2neo class to handle specifics of ETL from Mongo to Neo4j."""

    #neo4j_uri = 'http://localhost:7474/db/data/'

    properties_to_delete = ['_id', 'video_embeds', 'web_presences', 'degrees', 'relationships', 'external_links',
                            'milestones', 'investments', 'image','funds', 'funding_rounds', 'providerships',
                            'tag_list', 'offices', 'partners', 'products', 'screenshots', 'competitions',
                            'acquisitions', 'acquisition', 'ipo']

    # TODO: add acquisition and ipo to graphs

    def get_collection(self, db_name, collection_name, host='localhost', port=27017):
        """Given database and collection names returns a collection."""
        print 'get_collection', db_name, collection_name, host, port
        mc = MongoClient(host, port)
        return mc[db_name][collection_name]

    def add_node_collection_to_graph(self, db_name, collection_name, label, limit=0):
        c = self.get_collection(db_name, collection_name)
        cur = c.find(limit=limit)
        batch = neo4j.WriteBatch(self)
        for i, d in enumerate(cur):
            anode = self.get_or_add_node_to_batch(d, label_index=label, batch=batch, stub='False')
            if (i % 100) == 0:
                batch.submit()
                batch = neo4j.WriteBatch(self)
                print 'add_node_collection_to_graph', label, i, self.order
        batch.submit()
        #return anode

    def get_or_add_node_to_batch(self, node_dict, label_index, batch, stub='', create=True):
        """
        Given a node dictionary gets an existing node, or creates one and returns it.

        Batch node cannot be used for creating relationships in batch (use create=True).
        Assumes dictionary is stub/visited is set to False.

        node_dict: dictionary of properties
        label_index: label for node and index used
        stub: True if being created without full properties (i.e., as part of a relationship)
        create: if true the node is created immediately, otherwise it is added to the batch
                if the node already exists this is ignored
        returns: a node or BatchObject representing a node
        """
        key = 'permalink'
        value = str(node_dict[key])
        anode = self.get_indexed_node(label_index, key, value)
        if anode:
            anode.update_properties(node_dict)
        # If node did not already exist, create it or add it to the batch
        if not anode:
            node_properties = self.get_node_properties_from_dictionary(node_dict)
            if create:
                node_properties.update({'visited': 'False', 'stub': stub})
                anode = self.get_or_create_indexed_node(label_index, key, value, node_properties)
                anode.add_labels(label_index)
            else:
                anode = batch.get_or_create_in_index(neo4j.Node, label_index, key, node(node_properties))
                batch.add_labels(anode, label_index)
                batch.set_properties(anode, {'visited': 'False', 'stub': stub})
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

    def add_edges_to_graph(self, db, collection, index='funder', edge_names=[], relationship_type='funded', limit=0):
        """
        Assumes that funder nodes have been created.
        db: Mongo database
        collection: Collection in the database
        index: index for funder nodes (funder or person)
        relationship_type: type of relationship being added
        limit: maximum records to retrieve from Mongo, if 0 all are retrieved
        """
        # Define how often to print status messages
        node_status_freq = {'funder':100,'person':2000, 'milestones':100}
        edge_names_by_node_type = {'funder': ['investments'],
                                   'person': ['investments', 'relationships'],
                                   'company': []
                                   }

        c = self.get_collection(db, collection)
        cur = c.find(limit=limit)
        cur.batch_size(20)
        batch = neo4j.WriteBatch(self)
        print 'Number of Nodes to Investigate ', db, collection, ':', cur.count()

        # Iterate over node collection
        for i, d in enumerate(cur):
            if (i % node_status_freq[index]) == 0:
                batch.submit()
                neo4j.WriteBatch(self)
                print 'Adding', index, 'relationships, next iter: ', index, i
            ########item_list = d[index]
            print 'adding {} relationships from {}.'.format(index, d['permalink'])

            # Get the source node, if it doesn't exist then create one using current properties
            source_node = self.get_indexed_node(index, 'permalink', d['permalink'])
            if not source_node:
                source_properties = self.get_node_properties_from_dictionary(d)
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

    def add_relationships_to_graph(self, source_node, prop_dict, batch, relationship_type=''):
        """
        Add funding round relationships for one investor (financial org or person).

        Assumes a company is receiving the funds.
        self: graph object
        source_node: source node for relationship
        prop_dict: single dictionary from source's list
        batch: neo4j WriterBatch
        relationship_type: Type for created relationship-varies for relationships #####
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
        if title:
            path = batch.get_or_create_path(source_node, (title, prop_dict), target_node)

        return None

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
        company_node = self.get_or_add_node_to_batch(company_dict, 'company', batch=batch, create=True)
        del investment_dict['funded_month']
        del investment_dict['funded_day']
        del investment_dict['company']          # leave d with properties for relationship

        path = batch.get_or_create_path(funder_node, (relationship_type, investment_dict), company_node)
        return None

    def date_from_dictionary(self, d, prefix):
        """Derive a date string from dictionary entries for day, month, and year, return string."""
        datestr = str(d[prefix+'_year']) + '-' + str(d[prefix+'_month']) + '-' + str(d[prefix+'_day'])
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
        return cnt

    def get_node_index(self, index_name):
        return self.get_or_create_index(neo4j.Node, index_name)

    def get_relationship_index(self, index_name):
        return self.get_or_create_index(neo4j.Relationship, index_name)

    def export_person_nodes_to_csv(self, out_file_name='person_nodes.tab', limit=9999999):
        node_type = 'person'
        query_str = 'match (n:' + node_type + ') ' + ' return n'
        initial_dict = {'label': node_type}
        person_fields = [u'nodes', u'id', u'label', u'first_name', u'last_name', u'affiliation_name',u'created_at',
                         u'updated_at', u'twitter_username', u'blog_feed_url', u'blog_url', u'alias_list',
                         u'born_month',u'crunchbase_url', u'homepage_url', u'born_day', u'born_year']
        result = CypherQuery(self, 'match (n:' + node_type + ') ' + ' return count(n);').execute()
        count, = result.data[0].values
        count = min(count, limit)
        print 'count person nodes', count
        self.export_nodes_to_csv(node_type, query_str, count, out_file_name, person_fields, initial_dict, sep='\n')

    def export_company_node_to_csv(self, out_file_name='company_nodes.tab', limit=9999999):
        node_type = 'company'
        query_str = 'match (n:' + node_type + ') ' + ' return n '
        initial_dict = {'label': node_type}
        company_fields = [u'nodes', u'id', u'label', u'name', u'category_code', u'crunchbase_url', u'description',
                           u'number_of_employees', u'created_at', u'updated_at', u'founded_day', u'alias_list',
                           u'deadpooled_month', u'deadpooled_year', u'deadpooled_day', u'deadpooled_url',
                           u'twitter_username', u'homepage_url', u'total_money_raised', u'blog_url', u'error',
                           u'blog_feed_url', u'founded_month', u'email_address', u'founded_year']
        result = CypherQuery(self, 'match (n:' + node_type + ') ' + ' return count(n);').execute()
        count, = result.data[0].values
        count = min(count, limit)
        print 'count company nodes', count
        self.export_nodes_to_csv('company', query_str, count, out_file_name, company_fields, initial_dict, sep='\n')

    def export_financial_nodes_to_csv(self, out_file_name='financial_nodes.tab', limit=9999999):
        node_type = 'funder'
        query_str = 'match (n:' + node_type + ') ' + ' return n '
        initial_dict = {'label': node_type}
        funder_fields = [u'nodes', u'id', u'label', u'name', u'permalink', u'crunchbase_url', u'homepage_url',
                         u'blog_url', u'blog_feed_url', u'description', u'overview', u'twitter_username',
                         u'phone_number',u'email_address', u'founded_month', u'founded_year', u'created_at',
                         u'updated_at', u'founded_day', u'alias_list', u'tag_list', u'deadpooled_month',
                         u'deadpooled_year', u'deadpooled_day', u'deadpooled_url', u'total_money_raised', u'error',]
        result = CypherQuery(self, 'match (n:' + node_type + ') ' + ' return count(n);').execute()
        count, = result.data[0].values
        count = min(count, limit)
        print 'count financial nodes', count
        self.export_nodes_to_csv(node_type, query_str, count, out_file_name, funder_fields, initial_dict, sep='\n')

    def export_funded_relationships_to_csv(self, out_file_name='funded_relations.tab', limit=9999999):
        rel_type = 'funded'
        query_str = 'match (a)-[r:' + rel_type + ']->(b) ' + ' return a.permalink as source, r, b.permalink as target, id(r) as id'
        initial_dict = {'label': rel_type, 'source_id': ''}
        funded_fields = [u'source', u'target', u'type', u'source_id', u'id', u'label', u'name', u'category_code',
                         u'crunchbase_url', u'funded_month', u'source_description', u'round_code', u'raised_amount',
                         u'source_url', u'raised_currency_code', u'funded_year', u'funded_day']
        result = CypherQuery(self, 'match ()-[r:' + rel_type + ']->() ' + ' return count(r);').execute()
        count, = result.data[0].values
        count = min(count, limit)
        print 'Count rels', count
        self.export_relations_to_csv('funded', query_str, count, out_file_name, funded_fields, initial_dict, sep='\n')

    def export_relations_to_csv(self, type, query_str, count, out_file, fields, initial_dict={}, sep=','):
        """
        type: person, funder, or company
        query_str: cypher query string to return the nodes or relations
        out_file: output file
        fields: fields to write
        initial_dict: dict with any vars not in node
        sep: separator to use in output file
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
                print 'first, query_str_with_limits', first, ' :: ', query_str_with_limits
                try:
                    query = CypherQuery(self, query_str_with_limits)
                    for relationship in query.stream():
                        relation_str = relationship.values[1]
    # you can use extract to get node and relationship properties
    #
    #
    # return extract(r in rels(path) : r.foo)
    # or
    # return extract(n in nodes(path) : n.bar)

                        pat = re.compile("""\(([0-9]{4,8})\)                    # [0]Source id
                                            [-<]{1,2}\[\:                       # Pointer
                                            ([0-9a-zA-Z-_]{2,40})[ ]?           # [1]Relationship type
                                            (\{.*\})                            # [2]Properties dictionary
                                            \][->]{1,2}                         # Pointer
                                            \(([0-9]{4,8})\)?""", re.X)         # [3]Target id

                        try:
                            match_obj = re.search(pat, str(relation_str))
                            if not match_obj:
                                print 'Error matching relationship ', ':' + relation_str + ':'
                                continue

                            d = initial_dict
                            d['source'] = match_obj.groups()[0]
                            d['label'] = match_obj.groups()[1]
                            d['type'] = match_obj.groups()[1]
                            d['target'] = match_obj.groups()[3]
                            pseudo_dict = match_obj.groups()[2].replace('""', '')

                            for item in re.findall('(\w*)":("\w*"|[0-9.]{1,15})', pseudo_dict):
                                d[list(item)[0]] = list(item)[1]

                            if not d.has_key('permalink'):
                                if d.has_key('crunchbase_url'):
                                    d['permalink'] = d['crunchbase_url'].split('/', -1)[-1]
                                else:
                                    d['permalink'] = uuid.uuid4()
                            d['id'] = d['permalink']
                            for key in d:
                                field_set.add(key)

                            dw.writerow(d)
                            n_exported += 1
                            if (n_exported % 1000) == 0:
                                print 'Relationships exported: ', n_exported
                        except UnicodeEncodeError as uee:
                            n_errors += 1
                            print 'Unicode Error Inside', uee.args
                        except ValueError as err:
                            n_errors += 1
                            print 'Unknown Error Inside', err.args

                except UnicodeEncodeError as uee:
                    n_errors += 1
                    print 'Unicode Error Outside', uee.args
                except OutOfMemoryError as oome:
                    print 'Out of Memory Error', oome.args
                    return
                except ValueError as err:
                    n_errors += 1
                    print 'Unknown Error Outside', err.args

        print 'Fields not used in ', type, ':', field_set - header_set
        print 'Done with export of ', type
        print '     Exported: ', n_exported
        print '     Errors:   ', n_errors

    def export_nodes_to_csv(self, type, query_str, count, out_file, fields, initial_dict={}, sep=','):
        """
        type: person, funder, or company
        query_str: cypher query string to return the nodes or relations
        out_file: output file
        fields: fields to write
        initial_dict: dict with any vars not in node
        sep: separator to use in output file
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
                print 'first, query_str_with_limits', first, ' :: ', query_str_with_limits
                try:
                    print 'q string', query_str
                    query = CypherQuery(self, query_str_with_limits)
                    for item in query.stream():
                        for anode in item:
                            try:
                                d = initial_dict
                                if not anode['permalink'] and anode['crunchbase_url']:
                                    anode['permalink'] = anode['crunchbase_url'].split('/', -1)[-1]
                                d['nodes'] = anode['permalink']
                                d['id'] = anode._id
                                d.update(node.get_properties())
                                for key in node:
                                    field_set.add(key)
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
                            except:
                                print 'Unknown Error Inside on Nodes'
                except UnicodeEncodeError as uee:
                    n_errors += 1
                    print 'Unicode Error Outside on Nodes', uee.args
                except OutOfMemoryError as oome:
                    print 'Out of Memory Error', oome.args
                    return
                except ValueError as ve:
                    n_errors += 1
                    print 'Unknown Error Outside on Nodes', ve.args

        print 'Fields not used in ', type, ':', field_set - header_set
        print 'Done with export of ', type
        print '     Exported: ', n_exported
        print '     Errors:   ', n_errors
