
#-------------------------------------------------------------------------------
# Name:        neo4j_to_gephi.py (was graph_export.py)
# Purpose:     Exports data from neo4j in format for Gephi
# Author:      Casson
# Created:     3/24/2014
# Copyright:   (c) Casson 2014
#-------------------------------------------------------------------------------
__author__ = 'Casson'

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