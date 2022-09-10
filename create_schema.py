"""
In this module we design all nessary
function that create the database and its schema.

To build a star schema we have function to create and drop all the tables:

"""
import configparser
import psycopg2
from sql_queries import drop_queries, create_queries 


config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))


KEY                    = config.get('AWS','KEY')
SECRET                 = config.get('AWS','SECRET')

DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")

DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
DWH_DB                 = config.get("DWH","DWH_DB")
DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
DWH_PORT               = config.get("DWH","DWH_PORT")
DWH_ENDPOINT           = config.get("DWH","DWH_ENDPOINT")

DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")

def create_database():


    # connect to the existed database
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(DWH_ENDPOINT,DWH_DB,DWH_DB_USER,DWH_DB_PASSWORD,DWH_PORT))
    conn.set_session(autocommit=True)
    curs = conn.cursor()

    
    
    # return the connection and cursor to use in other functions

    return conn, curs
    

def drop_tables(conn,curs):

    for query in drop_queries:
        
        curs.execute(query)
        conn.commit()
        



def create_tables(conn,curs):

    for query in create_queries:
        
        curs.execute(query)
        conn.commit()

