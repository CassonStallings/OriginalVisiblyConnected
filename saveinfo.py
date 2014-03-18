#-------------------------------------------------------------------------------
# Name:        saveinfo.py
# Purpose:
# Author:      Casson
# Created:     11/03/2014
#-------------------------------------------------------------------------------

def main():
    pass

    import logging
    logging.basicConfig(level=logging.DEBUG)

    from py2neo import neo4j
    g = neo4j.GraphDatabaseService("http://localhost:7474/db/data/")

    from py2neo import node, rel
    movie = g.create(
        node(name='Willy'),
        node(name='bob'),
        rel(0, "linktype", 1))
    #g.clear()

if __name__ == '__main__':
    main()
