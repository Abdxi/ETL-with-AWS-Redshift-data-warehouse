"""
In this module, we desin all nessary functions that load data 
fro ms3 to staging tables an then from staging tables to the redshift database

"""


import os
import glob
import configparser
import boto3
import pandas as pd
from sql_queries import *


def staging_data(conn,curs):
    for query in staging_queries:
        cur.execute(query)
        conn.commit()

def insert_data(conn,curs):
    for query in insert_queries:
        cur.execute(query)
        conn.commit()





    




        

    








