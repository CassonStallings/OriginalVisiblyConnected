"""
Name:       crunchbase_to_mongo.py
Purpose:    Pulls data from MongoDB and exports to Neo4j as nodes and relations.
Requires:   GraphBuilder.py
Created:    3/12/2014
Copyright:  Casson Stallings (c) 2014
Licence:    Apache version 2.0
"""

from src.GraphBuilder import GraphBuilder

def main():
    # TODO Deal with unicode errors
    # TODO Too much junk is going to export relationships
    # TODO Are there dates in the neo4j database (as opposed to year month day)
    g = GraphBuilder('http://localhost:7474/db/data/')
    # g.export_person_nodes_to_csv(out_file_name='person_nodes.tab')
    # g.export_company_node_to_csv(out_file_name='company_nodes.tab')
    # g.export_financial_nodes_to_csv(out_file_name='financial_nodes.tab')
    g.export_funded_relationships_to_csv(out_file_name='funded_relations.tab')

if __name__ == '__main__':
    main()