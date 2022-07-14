## Google Cloud Function - Python
These scripts extract data from a source location (FTP server or MySQL DB) and pushes that data to a Bigquery data-warehouse.
- Note that the imported libraries need to be in a seperate file called "requirements.txt"
- Your application secrets should not be in the code but should be in a <a href="https://medium.com/geekculture/secure-your-credentials-in-google-cloud-functions-with-secret-manager-22a4a1b3788a"> secret manager</a> ideally.

These scripts are referenced in these articles:  
- <a href="https://towardsdatascience.com/building-a-simple-etl-pipeline-with-python-and-google-cloud-platform-6fde1fc683d5 
">Building a pipeline from an FTP server to BigQuery</a>  and 
- <a href="https://towardsdatascience.com/part-2-building-a-simple-etl-pipeline-with-python-and-google-cloud-functions-mysql-to-bigquery-4e1987f9f89b">Building a pipeline from a MySQL DB to BigQuery</a>  

