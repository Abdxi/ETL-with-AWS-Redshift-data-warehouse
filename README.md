## ETL with Redshift Data warehouse

#### Overview
In this project, the client has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

They need to build an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to.

#### Project description
Based on the client requirments we'll create a data warehouse with star schema designed to optimize queries on song play analysis.

the steps we follow stated below:
1. Build data warehouse and create schema
- Design STAR schema for fact and dimension tables
- Write all needed SQL statements in sql_queries.py
- Launch a redshift cluster and create an IAM role that has read access to S3
- Add redshift database and IAM role info to dwh.cfg
- Connect to redshift cluster and create tables in create_tables.py

2. Build ETL Pipeline
- Implement the logic in etl.py to load data from S3 to staging tables on Redshift
- Implement the logic in etl.py to load data from staging tables to analytics tables on Redshift


#### Dataset
There are two types of data files used in this project

##### Song Dataset:

The first dataset is a subset of real data from the Million Song Dataset. Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are filepaths to two files in this dataset.

`song_data/A/B/C/TRABCEI128F424C983.json`
`song_data/A/A/B/TRAABJL12903CDCF1A.json`

##### Log Dataset:
These files (in JSON format) contain user activeity data on the client new music streaming app.

Here are filepaths to two files in this dataset.

`log_data/2018/11/2018-11-12-events.json`
`log_data/2018/11/2018-11-13-events.json`

### Database Schema
We used a star schema used for this project to optimize the nalaytics jobs
There is a fact table that contain all the user activity measures, and 4 dimentional tables (users, artists, songs and time) referenced from the fact table by the primary keys.

![alt text](https://github.com/Abdxi/ETL-with-AWS-Redshift-data-warehouse/blob/main/images/database%20schema.png)
