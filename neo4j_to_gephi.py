#-------------------------------------------------------------------------------
# Name:        neo4j_to_gephi.py (was graph_export.py)
# Purpose:     Exports data from neo4j in format for Gephi
# Author:      Casson
# Created:     3/24/2014
# Copyright:   (c) Casson 2014
#-------------------------------------------------------------------------------
__author__ = 'Casson'

import copy
from py2neo import neo4j as Neo
from py2neo import cypher
from py2neo import node, rel
from pymongo import MongoClient
from py2neo.neo4j import GraphDatabaseService
from py2neo.neo4j import CypherQuery
import csv
import re
import uuid

def main():

    g = GraphDatabaseService('http://localhost:7474/db/data/')

    node_type = 'person'
    base_str = 'match (n:' + node_type + ') '
    query_str = base_str + ' return n'
    count_str = base_str + 'return count(n);'
    out_file = 'person_nodes.tab'
    initial_dict = {'label': node_type}
    person_fields = [u'nodes', u'id', u'label', u'first_name', u'last_name', u'affiliation_name',u'created_at',
                     u'updated_at', u'twitter_username', u'blog_feed_url', u'blog_url', u'alias_list',
                     u'born_month',u'crunchbase_url', u'homepage_url', u'born_day', u'born_year']
    #export_nodes_to_csv(g, 'person', query_str, count_str, out_file, person_fields, initial_dict, sep='\n')

    node_type = 'company'
    base_str = 'match (n:' + node_type + ') '
    query_str = base_str + ' return n '
    count_str = base_str + ' return count(n);'
    out_file = 'company_nodes.tab'
    initial_dict = {'label': node_type}
    company_fields = [u'nodes', u'id', u'label', u'name', u'category_code', u'crunchbase_url', u'description',
                       u'number_of_employees', u'created_at', u'updated_at', u'founded_day', u'alias_list',
                       u'deadpooled_month', u'deadpooled_year', u'deadpooled_day', u'deadpooled_url',
                       u'twitter_username', u'homepage_url', u'total_money_raised', u'blog_url', u'error',
                       u'blog_feed_url', u'founded_month', u'email_address', u'founded_year']
    #export_nodes_to_csv(g, 'company', query_str, count_str, out_file, company_fields, initial_dict, sep='\n')

    rel_type = 'funded'
    base_str = 'match (a)-[r:' + rel_type + ']->(b) '
    query_str = base_str + ' return a.permalink as source, r, b.permalink as target, id(r) as id '
    count_str = base_str + ' return count(r);'
    out_file = 'funded_relations.tab'
    initial_dict = {'label': rel_type, 'source_id': '', }
    funded_fields = [u'source', u'target', u'type', u'source_id', u'id', u'label', u'name', u'category_code',
                     u'crunchbase_url', u'funded_month', u'source_description', u'round_code', u'raised_amount',
                     u'source_url', u'raised_currency_code', u'funded_year', u'funded_day']
    #export_relations_to_csv(g, 'funded', query_str, count_str, out_file, funded_fields, initial_dict, sep='\n')


def export_relations_to_csv(self, type, query_str, count_str, out_file, fields, initial_dict={}, sep=','):
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


    count_res = CypherQuery(self, count_str).execute()
    print 'count_res data', count_res.data
    count = 55584

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
                                        \(([0-9]{4,8})\)?""", re.X)                # [3]Target id

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
            except OutOfMemoryError as omme:
                print 'Out of Memory Error', omme.args
                return
            except ValueError as err:
                n_errors += 1
                print 'Unknown Error Outside', err.args

    print 'Fields not used in ', type, ':', field_set - header_set
    print 'Done with export of ', type
    print '     Exported: ', n_exported
    print '     Errors:   ', n_errors


def export_nodes_to_csv(self, type, query_str, count_str, out_file, fields, initial_dict={}, sep=','):
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

    count = 161000
    count_res = CypherQuery(self, count_str).execute()
    print 'count_res.data', count_res.data

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
                    for node in item:
                        try:
                            d = initial_dict
                            if not node['permalink'] and node['crunchbase_url']:
                                node['permalink'] = node['crunchbase_url'].split('/', -1)[-1]
                            d['nodes'] = node['permalink']
                            d['id'] = node._id
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
            except OutOfMemoryError as omme:
                print 'Out of Memory Error', omme.args
                return
            except ValueError as ve:
                n_errors += 1
                print 'Unknown Error Outside on Nodes', ve.args

    print 'Fields not used in ', type, ':', field_set - header_set
    print 'Done with export of ', type
    print '     Exported: ', n_exported
    print '     Errors:   ', n_errors


if __name__ == '__main__':
    main()