# ETL USING REDSHIFT AND S3
## Context Problem
For a start-up with a large collection of data on songs and user activity on amazon S3.
An analytics team is required to perform operations leading to discovery, interpretation, and communication of meaningful patterns in data, vto do this various queries has to be carried out on their data, which resides in a directory of JSON logs, as well as a directory with JSON metadata on the songs.
Currently, there isn't an easy way to carry out this query.

## Solution
The solution proposed in this repo is to create a data warehouse for OLAP by building 
an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a
set of dimensional tables for the analytics team.

## File Structure
1. *sql_queries.py* contains the sql queries used.
2. *create_tables.py* resets the database(drops and create table).
3. *connection_manger.py* contains context manager to help manage cursors and connections to the database including decorators
for error handling.
4. *etl.py* extracts, transforms and loads data  into tables
5. *cluster_manager.py* Manages connections to AWS resources such as S3, RedShift, EC2.
6. *config.py* configurations for the project.
