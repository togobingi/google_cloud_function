#### This file extracts data from a MySQL database and loads it into a BQ table ###

#Import packages 
import MySQLdb 
import pandas as pd
import numpy as np
from sshtunnel import SSHTunnelForwarder
from google.cloud import storage
from google.cloud import bigquery
import pyarrow

    
def handler(request):
    ############### EXTRAT DATA ##################
    #ssh variables
    host = 'SSH_HOST_NAME'
    localhost = 'LOCALHOST'
    ssh_username = 'SSH-USERNAME'
    ssh_password = 'SSH_PASSWORD'

    #database variables
    user = 'user'
    password = 'password'
    database = 'DB_NAME'

    #The Query Function
    def query(q):
        with SSHTunnelForwarder(
            (host,22),
            ssh_username=ssh_username,
            ssh_password=ssh_password,
            remote_bind_address=(localhost, 3306)
        ) as server:
            conn = MySQLdb.connect(host=localhost,
                                    port=server.local_bind_port,
                                    user=user,
                                    passwd=password,
                                    db=database)
            return pd.read_sql_query(q, conn)
        conn.close()
        SSHTunnelForwarder.close


    df = query('SELECT * FROM mysql_table_name ORDER BY id')

    
    # from google.cloud import bigquery
    client = bigquery.Client()
    dataset_id = 'dataset_ID'

    dataset_ref = client.dataset(dataset_id)
    job_config = bigquery.LoadJobConfig()
    job_config.autodetect = True
    job_config.write_disposition = "WRITE_TRUNCATE"
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    load_job = client.load_table_from_dataframe(df, dataset_ref.table("bigQuery_table_name"), job_config=job_config, location="dataset_location")    # API request
    print("Starting job {}".format(load_job))
    return ("Done!", 200)
