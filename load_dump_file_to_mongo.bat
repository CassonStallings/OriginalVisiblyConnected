# back up data
mongodump --collection companies --db crunchbase
mongodump --collection people --db crunchbase
mongodump --collection financial_organizations --db crunchbase
mongodump --collection products --db crunchbase
mongodump --collection service_providers --db crunchbase


# restoring data to single database named crunchbase

c:\mongodb\bin\mongorestore --drop C:\Users\Casson\Desktop\Startups\Data\mongo_dump_2014Mar24


