# Visibly Connected I#

Visibly connected was originally created as a two-week capstone 
project while I was at the Zipfian Academy. The software here integrates the 
complete Crunchbase database into both Neo4j and Gephi for analysis and exploration.

The work of Visibly Connected I is being continued in Visibly Connected II with 
a Crunchbase class for the Crunchbase API version 2, more emphasis on the 
analytics, and less on using MongoDB, and Neo4j.

## Files ##
### Presentation and Figures ###
*   **VisiblyConnectedSchema.pptx**
    Schema of Neo4j Database.
*   **VisiblyConnectedOriginalPresentation.pptx**
    Slides from three minute presentation on visibly connected.

### Code ###
*   **crunchbase_to_mongo.py**
    Pulls data from crunchbase through the API and stores JSON
    documents in MongoDB which may be running locally or on AWS. It is
    set up to get all data, not data related to specific
    queries. Relies on CrunchbaseApi.py.
    
*   **mongo_to_neo4j.py**
    Cleans Crunchbase data in mongo and creates a neo4j graph database.
       
*   **crunchbase_to_mongo.py**
    Pulls data from MongoDB and exports to Neo4j as nodes and relations.
    
*   **CrunchbaseApi.py:** 
    Class with tools to pull data from crunchbase through the V1.0 API
    and store it in MongoDB as JSON documents. MongoDB may be running
    on AWS. The class is set up to get all data, not data related to specific
    queries. This is primarily used by crunchbase_to_mongo.py. It can be used
    separately, but the mongoDB is integrated a little too well.

    Connection pooling is used to allow multiple simultaneous requests. The
    frequency of requests is limited to 10 per second per the Crunchbase API.
 
*   **GraphBuilder.py:**
    Class extending py2neo with tools to create a Neo4j graph database 
    using Crunchbase data stored in MongoDB or to export Neo4j database 
    for import to Gephi.

*   **path_display.py**
    Initial cut at displaying partial networks in Networkx.
    
*   **load_dump_file_to_mongo.bat**
    Basic commands necessary to export MongoDB data to a dump file and import the 
    same into a MongoDB collections on another machine. 
    Will not actually work as a batch file.
    
### Pickled Entity Lists ###
These are the complete permalink lists from Crunchbase as of April 2014.
*   **company_list_2014Apr13.pkl**
*   **financial-organization_list_2014Apr13.pkl**
*   **person_list_2014Apr13.pkl**
*   **product_list_2014Apr13.pkl**
*   **service-provider_list_2014Apr13.pkl**

### Gephi Import Files ###
These files represent the companies, financial entities, and people that are directly related
funding events. They are circa April 2014 and suitable for import into Gephi.
*   **company_notes.tab**
*   **financial_nodes.tab**
*   **funded_relations.tab**
*   **person_nodes.tab**
