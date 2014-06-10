#-------------------------------------------------------------------------------
# Name:        mongo_to_neo4j.py
# Purpose:     extends py2neo Graph class with methods specific to retrieving
#                crunchbase data from MongoDB and putting it in neo4j.
#                It performs ETL, cleaning data in mongo and structuring
#                it in neo4j using py2neo. The main program carries out the
#                the work using the graph class.
# Author:      Casson Stallings
# Created:     3/12/2014
# Copyright:   (c) Casson 2014
# Licence:     <your licence>
#-------------------------------------------------------------------------------

from src.GraphBuilder import GraphBuilder

def main():
    # Get reference to neo4j
    g = GraphBuilder('http://localhost:7474/db/data/')

    # uncomment to start with fresh database
    g.clear()
    #
    # print 'Neo4j Version', g.neo4j_version
    #
    # # Grab all nodes from mongo database and  add to graph database
    g.add_node_collection_to_graph('crunchbase', 'financial_organizations', 'funder', limit=200)
    g.add_node_collection_to_graph('crunchbase','people', 'person', limit=200)
    g.add_node_collection_to_graph('crunchbase', 'companies', 'company', limit=200)
    #
    # # Add funding rounds by financial organizations and individuals
    g.add_edges_to_graph('crunchbase', 'companies', index='company', limit=200)  ##  relationship_type='funded', limit=200)
    g.add_edges_to_graph('crunchbase', 'financial_organizations', index='funder', limit=200)   ## relationship_type='funded', limit=200)
    g.add_edges_to_graph('crunchbase', 'people', index='person', limit=200)  ##  relationship_type='funded', limit=200)

    print 'Nodes in graph', g.order

if __name__ == '__main__':
    main()
