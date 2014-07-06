# Basic commands necessary to export MongoDB data to a dump file and import
# the same into a MongoDB collections on another machine. Obviously
# you will need to transfer the data.

# Generate dump files

mongodump --collection companies --db crunchbase
mongodump --collection people --db crunchbase
mongodump --collection financial_organizations --db crunchbase
mongodump --collection products --db crunchbase
mongodump --collection service_providers --db crunchbase

# Restore data to single database named crunchbase on new machine

c:\mongodb\bin\mongorestore --drop C:\Users\Casson\Desktop\Startups\Data\mongo_dump_2014Mar24


