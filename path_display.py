#-------------------------------------------------------------------------------
# Name:        path_display.py (was graph_export.py)
# Purpose:     Exports data from neo4j in format for Gephi
# Author:      Casson
# Created:     3/24/2014
# Copyright:   (c) Casson 2014
#-------------------------------------------------------------------------------
__author__ = 'Casson'


import copy
import py2neo
from py2neo import neo4j as neo
from py2neo import cypher
from py2neo import node, rel
from pymongo import MongoClient
import networkx as nx
import pylab as plt
import re
import cProfile
import cPickle
import numpy as np
import graphviz


def main():
    source_name = 'colin-rhodes'
    target_name = 'ben-horowitz'

    # py2neo graph representing neo4j
    #g = neo.GraphDatabaseService('http://localhost:7474/db/data/')

    # NetworkX graph
    with open(r'c:\users\casson\desktop\startups\data\graph_nodes.pkl') as fil:
        gnx = cPickle.load(fil)

    nl1 = nx.Graph().add_path(['greg-mervine', 'bain-co', 'anne-bonaparte', 'hewlett-packard', 'ben-horowitz'])
    nl2 = nx.Graph().add_path(['colin-rhodes', 'silicon-valley', 'atiq-raza', 'khosla-ventures', 'okta', 'ben-horowitz'])
    nl3 = nx.Graph().add_path(['alex-popescu', 'rethinkdb', 'andreessen-horowitz', 'ben-horowitz'])

    gnx = gnx.subgraph(nl2)
    # ego = nx.ego_graph(gnx, 'ben-horowitz', radius=4, undirected=True, )
    # print 'Bens Node Count ', nx.number_of_nodes(gnx)
    # print 'Bens Edge Count ', nx.number_of_edges(gnx)
    # with open(r'c:\users\casson\desktop\startups\data\ben4.pkl', 'w') as fil:
    #     gnx = cPickle.dump(fil)
    # Get data from neo4j and place in the networkX graph
    # save_file = r'c:\users\casson\desktop\startups\data\network_appio_only.pkl'
    # if True:
    #     gnx = neo4j_to_networkx(g, gnx, source="s:company {name: 'Apptio'}", target="t:company {name: 'Eventbrite'}", limit=1000)
    #     #gnx = neo4j_to_networkx(g, gnx, source="n", target=
    #     with open(save_file, 'w') as p:
    #         x = cPickle.dump(gnx, p)
    # else:
    #     with open(save_file, 'r') as p:
    #         qnz = cPickle.load(p)
    cnt = 0
    nlist = []
    # for n in nx.bfs_edges(gnx, 'ben-horowitz'):
    #     nlist.append(n)

    # for n, d in gnx.degree_iter():
    #     print  d, n
    #     cnt += 1
    #     if cnt > 2000:
    #         break
    #ego = nx.ego_graph(gnx, 'ben-horowitz', radius=2, undirected=True, )

    #
    # gnx1 = gnx
    # gnx = ego
    #gnx = cPickle.load(open('network_1.pkl', 'rb'))
    #print 'average connectivity', nx.average_node_connectivity(gnx)
    print 'Node Count ', nx.number_of_nodes(gnx)
    print 'Edge Count ', nx.number_of_edges(gnx)
    # nx.degree_histogram(gnx)
    # plt.show()
    # unx = nx.Graph(gnx)
    # for s in ['greg-mervine', 'colin-rhodes', 'alex-popescu']:
    #     print s, nx.shortest_path(unx, s, 'ben-horowitz')


    # Get the source and target nodes
    for n in gnx:
        if gnx.node[n].has_key('permalink') and gnx.node[n]['permalink'] == source_name:
            source_id = n
            source_node = gnx.node[n]
            print 'source node found'
        elif gnx.node[n].has_key('permalink') and gnx.node[n]['permalink'] == target_name:
            target_node  = gnx.node[n]
            target_id = n
            print 'target node found', target_name

    #####MAKE SURE IM GETTING A PATH


    # paths = get_path(gnx, source_node, target_node)
    # for i in paths:
    #     print i

    # #paths = all_shortest_paths(gnx, source, target, weight=None)   #based on hops only
    # print 'order', gnx.order()

   # edge width is proportional to weight
    print gnx
    print 'info', nx.info(gnx)
    edgewidth=[]
    for (u,v,d) in gnx.edges(data=True):
        width = 1
        edge = gnx.get_edge_data(u,v)
        if 'weight' in edge:
            width = edge['weight']
        edgewidth.append(width)

   #.savefig('c:\users\casson\desktop\startups\plot.png')
    # try:
    #pos = nx.draw_spectral(gnx, weight='weight')  #pos=nx.graphviz_layout(gnx)
    # except:
    #pos = nx.graphviz_layout(gnx)
    pos = nx.spring_layout(gnx,iterations=30)

    #posp1 = nx.spring_layout(p1, iterations=10)

    #pos = nx.shell_layout(gnx)

    #plt.rcParams['text.usetex'] = False
    #Node sizes and colors
    nodesize = list()
    nodecolor = list()
    labels = dict()
    for n in gnx:
        label = ''
        if n == source_id:
            print 'found start node'
            nodesize.append(400)
            nodecolor.append('b')
            if 'name' in gnx.node[n]:
                labels[n] = gnx.node[n]['name']
        # elif n in ['greg-mervine', 'colin-rhodes', 'elliot-roazen', 'alex-popescu']:
        #     nodesize.append(300)
        #     labels[n] = n
        elif n == target_id:
            print 'found target node'
            nodesize.append(400)
            nodecolor.append('r')
            if 'name' in gnx.node[n]:
                labels[n] = gnx.node[n]['name']
        else:
            nodesize.append(3)
            nodecolor.append('k')
    print labels
    plt.figure(figsize=(18, 12))
    #nx.draw_networkx_edges(gnx, pos, alpha=0.3, width=edgewidth, edge_color='m', arrows=False)
    nx.draw_networkx_nodes(gnx, pos, alpha=0.4, node_size=nodesize, node_color=nodecolor)
    nx.draw_networkx_edges(gnx, pos, alpha=0.4, node_size=0, width=1, edge_color='0', arrows=False)
    nx.draw_networkx_labels(gnx, pos, labels=labels, fontsize=10)
    # ##nx.draw_networkx_nodes(paths, pos, alpha= .5, node_size=700)


    # plt.figure(figsize=(18, 12))
    # nx.draw_networkx_edges(p2, pos, alpha=0.3, width=edgewidth, edge_color='m', arrows=False)
    # nx.draw_networkx_nodes(p2, pos, alpha=0.4, node_size=nodesize, node_color=nodecolor)
    # nx.draw_networkx_edges(p2, pos, alpha=0.4, node_size=0, width=1, edge_color='0', arrows=False)
    # nx.draw_networkx_labels(p2, pos, labels=labels, fontsize=10)
    # ##nx.draw_networkx_nodes(paths, pos, alpha= .5, node_size=700)

    font = {'fontname' : 'Helvetica', 'color' : 'k', 'fontweight' : 'bold', 'fontsize' : 10}
    plt.title("Shortest Path Between Source and Target", font)

    # change font and write text (using data coordinates)
    font = {'fontname': 'Helvetica', 'color': 'r', 'fontweight': 'bold', 'fontsize': 14}
    plt.text(0.5, 0.97, "edge width = Likelihood of Success for Transmittal", horizontalalignment='center',
             transform=plt.gca().transAxes)
    plt.text(0.5, 0.94,  "Source and Target Nodes are Large", horizontalalignment='center', transform=plt.gca().transAxes)

   # plt.axis('off')
    plt.savefig("path.png",dpi=75)

    print("Wrote path.png")
    plt.show() # display

    print 'DONE'

def get_path(gnx, source_node, target_node):
    unx = nx.Graph(gnx)
    paths = nx.shortest_path(unx, source_node, target_node)
    return paths

#def neo4j_to_networkx(neo_graph, x_graph, source="n:company {name: 'Apptio'}", target="p:company {name: 'Eventbrite'}"):
def neo4j_to_networkx(neo_graph, x_graph, source, target, limit=1000):

    # Query the neo4j database to  pull out a subset of the data    #start n=node:company (permalink = 'apptio')
    # q = neo.CypherQuery(neo_graph, """
    #                           match (n)-[r*2..3]-(p) return n, r, p;
    #                        """)

    qry_str = 'match (' + source + ')-[r*1..]-(' + target +' ), (t)-[c]-(a), (s)-[d]-(b) return * limit ' + str(limit) + ';'
    print 'query string', qry_str
    q = neo.CypherQuery(neo_graph, qry_str)

    alist = q.execute()
    #print 'alist info', type(alist), len(alist)
    for i, rec in enumerate(alist):
        #print 'rec', i, type(rec), rec
        #print 'values', i, type(rec.values), rec.values
        for relation in rec.values:
            if type(relation) not in (type([]), py2neo.neo4j.Node):
                print '\nFound a Relation that is not a node or list:', type(relation), relation
            if type(relation) == type([]):
               # print '\nProcessing relations list'
                for edge in relation:
                    try:
                        edge_to_networkx(x_graph, edge)
                    except TypeError as ty:
                        print '\nTYPEERROR: i:', i, ty.args
                        continue
    return x_graph

def edge_to_networkx(g, neo_edge):
    """Adds a neo4j edge and its nodes to networkX, no return.

    g: networkX graph
    newo_edge: edge from neo4j
    """
    source = neo_edge.start_node
    target = neo_edge.end_node
    node_to_networkx(g, source)
    node_to_networkx(g, target)
    edge_props = neo_edge.get_properties()
    edge_props['id'] = neo_edge._id
    g.add_edge(source._id, target._id, edge_props)

def node_to_networkx(g, neo_node):
    """Adds nodes from neo4j to a networkx graph, returns the graph."""
    id = neo_node._id
    props = neo_node.get_properties()
    return g.add_node(id, props)

def get_path(g, source, target):
    paths = g.match(start_node=source, end_node=None, bidirectional=True, limit=2)
    return paths




if __name__ == '__main__':
    main()