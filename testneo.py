#-------------------------------------------------------------------------------
# Name:        Testneo.py
# Purpose:
# Author:      Casson
# Created:     09/03/2014
#-------------------------------------------------------------------------------
#import json, get_logger, quote, urlencode, encode_dict
import bulbs
#from bulbs.rexster import graph
import bulbs.gremlin

from bulbs import neo4jserver
#from bulbs.model import Node, Relationship
#from bulbs.property import String, Integer, DateTime
#from bulbs.utils import current_datetime

def main():
    config = bulbs.base.Config("http://127.0.0.1:7474/db/data/")
    g = bulbs.neo4jserver.graph.Graph(config)

    #james = g.vertices.create(name="James2")
    #julie = g.vertices.create(name="Julie")
    #knows = g.edges.create(james, "knows", julie)

    a='''match (n) return n;'''

if __name__ == '__main__':
    main()
