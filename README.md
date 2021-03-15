# mongo-compare
This is a mongodb data comparison tool.

In the mongodb official tools, mongodb officially provides a series of tools such as mongodump, mongorestore, etc. These tools play a very important role in our operation and maintenance of the mongodb database. But in our practice, we often need to compare whether the data of the two mongodb databases are consistent.
For example, the following scenario:
1. Use mongorestore to restore data to another database
2. Use some data migration tools to synchronize data from one database to another database
In these scenarios, we need to confirm whether the new database is completely consistent with the original database to avoid data loss.

To this end, we have developed a new data comparison tool (mongo-compare) based on the framework of the official mongodb tool (https://github.com/mongodb/mongo-tools).
mongo-compare provides a comparison of two databases
The ability to compare the number of db, collection data, index, the number of records in the collection (count), etc. is currently supported. mongo-compare can also be used for full data comparison, partial data comparison and random data comparison.
