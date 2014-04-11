
#-------------------------------------------------------------------------------
# Name:        neo4j_to_gephi.py (was graph_export.py)
# Purpose:     Exports data from neo4j in format for Gephi
# Author:      Casson
# Created:     3/24/2014
# Copyright:   (c) Casson 2014
#-------------------------------------------------------------------------------
__author__ = 'Casson'

from code.GraphBuilder import GraphBuilder

def main():

    g = GraphBuilder('http://localhost:7474/db/data/')
    g.export_person_nodes_to_csv(out_file_name='person_nodes.tab', limit=10)
    g.export_company_node_to_csv(out_file_name='company_nodes.tab', limit=10)
    g.export_financial_nodes_to_csv(out_file_name='financial_nodes.tab', limit=10)
    g.export_funded_relationships_to_csv(out_file_name='funded_relations.tab', limit=10)


if __name__ == '__main__':
    main()