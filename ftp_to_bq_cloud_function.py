#import libraries
import wget
from ftplib import FTP
import datetime
import csv
from google.cloud import bigquery
import re



def bq_ftp(request):
    ftp = FTP("ftp-domain-url")
    ftp.login("ftp-username", "ftp-password")
    ftp.cwd("/directory-where-file-is") 
    
    #List files in folder and extract the most recent file (last modified)
    entries = list(ftp.mlsd())
    entries.sort(key = lambda entry: entry[1]['modify'], reverse = True)
    latest_file = entries[0][0]
    
    #Select most recent file in folder and append it to the FTP link
    recent_file = "ftp://ftp-username:ftp-password@ftp-domain-name/directory/"+ latest_file

    link = recent_file

    try:
        ftpfile = wget.download(link, out='/tmp/ANY-NAME.csv'') #save downloaded file in /tmp folder of Cloud Functions
        with open(ftpfile, 'r') as f:
            my_csv_text = f.read()
        find_str = 'FBA'  
        replace_str = 'AMAZON' 

        # replace all occurences of "FBA%" with "AMAZON" in csv
        new_csv_str = re.sub(find_str, replace_str, my_csv_text)

        # open new file and save
        new_csv_path = '/tmp/ANY-NAME-2.csv' # or whatever name you want
        with open(new_csv_path, 'w') as f:
            f.write(new_csv_str)
    except BaseException as error:
        print('An exception occurred: {}'.format(error))
    


	  #Create BQ client
    client = bigquery.Client()

    #BQ Table data
    dataset_id = 'BIGQUERY-DATASET-ID'
    table_id = 'BIGQUERY-TABLE-ID'


    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    job_config = bigquery.LoadJobConfig()
    job_config.field_delimiter=(";")
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.autodetect = True
    job_config.write_disposition = "WRITE_TRUNCATE"
    

    
    with open(new_csv_path, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_ref, job_config=job_config)
        print("Loading file to BigQuery...")
    source_file.close()
    
    job.result()  # Waits for table load to complete.
    print("The file has been successfully uploaded to BigQuery!")
