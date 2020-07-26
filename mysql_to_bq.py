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
    host = 'SRV-A-DE.C-589.MAXCLUSTER.NET'
    localhost = '127.0.0.1'
    ssh_username = 'web-user'
    ssh_password = 'sssh_password'

    #database variables
    user = 'user'
    password = 'password'
    database = 'coffeecircleshopware'

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


    df = query('SELECT * FROM s_order ORDER BY id')
    #Convert to json file
    #json_file = df.to_json(orient='records',lines="false") #convert sql results to json file
    
    # from google.cloud import bigquery
    client = bigquery.Client()
    dataset_id = 'mydataset'
    #table_id = "coffeecircle-dwh:mydataset.test_etl"

    dataset_ref = client.dataset(dataset_id)
    job_config = bigquery.LoadJobConfig()
    job_config.autodetect = True
    job_config.write_disposition = "WRITE_TRUNCATE"
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    load_job = client.load_table_from_dataframe(df, dataset_ref.table("test_etl"), job_config=job_config, location="US")    # API request
    print("Starting job {}".format(load_job))
    return ("Done!", 200)
