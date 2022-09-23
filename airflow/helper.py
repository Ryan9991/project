# --- Import packages and modules
import io
import pyarrow as pa
import pandas as pd
import numpy as np
import time, pytz, logging, subprocess, json, re, os, csv, shutil, requests
from google.cloud import bigquery, storage
from datetime import datetime, date, timedelta
from airflow.models import Variable
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.mysql_hook import MySqlHook
from airflow.hooks.base_hook import BaseHook

# --- This is helper class to support for ETL Operation 

# --- Function for sent notify to #data-notify channel
__slack_hook_channel = "#data-notify"


def set_slack_hook_channel(channel):
    """ 
    Get error messages into specified channel slack in every error DAG

    Params:
        channel (str) | Required: Name of channel

    Return:
        None, notify in slack channel

    Sample:
        set_slack_hook_channel("#data-notify-promoplan")

    """
    global __slack_hook_channel
    __slack_hook_channel = channel


def slack_hook(context):

    """ 
    Get error messages into #data-notify channel form slack in every error DAG

    Params:
        context (str) | Required: Messages of error context in every error DAG

    Return:
    The messages context with error template

    Sample:
        slack_hook

    """
    slack_msg   = "*Warning! Task Retry* on DAG: *" + context.get('task_instance').dag_id + "*"
    alert_color = "#FFFF00"
    alert_value = "Medium!"

    # --- Allow for maximum no retry
    if context.get('task_instance')._try_number <= 1 and context.get('task_instance').state != "failed" :
        logging.info("there is no context in error message")
        return "there is no context in error message"

    # --- Different alert style for retry and failed task
    if context.get('task_instance').state == "failed":
        alert_color = "#FF0000"
        alert_value = "High!!"
        slack_msg   = "*Attention!! Task Failed* on DAG: *" + context.get('task_instance').dag_id + "*"

    err_msg   = str(context['exception'])
    final_msg = (err_msg[:140] + '...') if len(err_msg) > 140 else err_msg
    
    # --- Define every line of error messages
    slack_attachment = [
        {
            "mrkdwn_in": ["text"],
            "color"    : alert_color,
            "fields"   : [
                {
                    "title": "Task Id: ",
                    "value": context.get('task_instance').task_id,
                    "short": True
                },
                {
                    "title": "Error Message: ",
                    "value": final_msg,
                    "short": True
                },
                {
                    "title": "Scheduled: ",
                    "value": str(datetime.fromtimestamp(context.get('task_instance').start_date.timestamp(), 
                    tz=pytz.timezone("Asia/Jakarta")).strftime('%Y-%m-%d %H:%M:%S')),
                    "short": True
                },
                {
                    "title": "Retry Count: ",
                    "value": str(context.get('task_instance')._try_number-1),
                    "short": True
                },
                {
                    "title": "Priority",
                    "value": alert_value,
                    "short": False
                },
                {
                    "value": "<"+str(context.get('task_instance').log_url)+ "|" + "Click here to see full log>",
                    "short": False
                },
            ],
            "footer"     : "Airflow Slack API",
            "footer_icon": "https://img2.pngio.com/apache-airflow-documentation-airflow-documentation-airflow-png-700_700.png",
            "ts"         : str(time.time())
        }
    ]

    connection        = BaseHook.get_connection('slack_connection')
    connection_string = 'login: {}\n password: {}\n host: {}\n port: {}\n'.format(str(connection.login), str(connection.password), str(connection.host), str(connection.port))
    logging.error(connection_string)

    # --- Hook the massage into slack operator
    slack_hook =  SlackWebhookOperator(
        task_id      = 'slack_hook',
        http_conn_id = 'slack_connection',
        message      = slack_msg,
        attachments  = slack_attachment,
        channel      = __slack_hook_channel,
        username     = 'airflow_local',
        icon_emoji   = None,
        link_names   = False,
        dag          = context.get('dag_run').dag
    )

    return slack_hook.execute(context=context)


def get_today():

    """
    Function for get today date

    Params:
        None

    Return:
        Today date format in date/timestamp data type

    Sample:
        get_today()
    """
    dateetl    = date.today()
    dateformat = dateetl.isoformat()

    return dateformat

def get_date(date_type, delta, timestamp=True, utc=0):

    """
    Function for get date with specific data type and specific time interval

    Params:
        date_type       (str) | Required: Type of starting/ending date to get
        delta           (int) | Required: Number of interval date
        timestamp   (boolean) | Required: Get type of date into timestamp (True)
        utc         (integer) | Required: Number of primary time standard for indonesia current time

    Return:
        Date time on timestamp data type for any requirements or intentions 

    Sample:
        get_date("start", delta=1, timestamp=False, utc=7)
    """

    # --- Stored today and yesterday date based on utc
    today = datetime.utcnow() + timedelta(hours=utc)

    yesterday = today - timedelta(days = int(delta))

    # --- Change timestamp format
    if timestamp:
        date_start = yesterday.strftime('%Y-%m-%d 00:00:00')
        date_end = today.strftime('%Y-%m-%d 23:59:59')
    else:
        date_start = yesterday.strftime('%Y-%m-%d')
        date_end = today.strftime('%Y-%m-%d')

    print("Extracting data from: " + date_start + " - " + date_end)

    # --- Stored datetime based on date type (start or end)
    if date_type == "start":
        return date_start
    else:
        return date_end

def schema_postgres_to_bq(connection, table_name, db_name, ingest_type):

    """
    Function for get schema PostgreSQL and change every data type into required data type on BigQuery

    Params:
        connection  (str) | Required: Name of DAG connection
        table_name  (str) | Required: Name of table
        db_name     (str) | Required: Name of database
        ingest_type (str) | Required: Type of ingestion

    Return:
        Dataframe that contain schema for desired table and database

    Sample:
        schema_postgres_to_bq(pg_conn, table, 'logistix', 'partial')
    """

    # --- Create query to change data type 
    request = f"""SELECT 'NULLABLE' AS mode, column_name AS name,
                CASE 
                    WHEN data_type = 'integer' OR data_type = 'bigint' THEN 'INTEGER'
                    WHEN data_type = 'double precision' THEN 'FLOAT'
                    WHEN data_type = 'timestamp' THEN 'TIMESTAMP'
                    WHEN data_type = 'timestamp without time zone' THEN 'TIMESTAMP'
                    WHEN data_type = 'timestamp with time zone' THEN 'TIMESTAMP'
                    WHEN data_type = 'boolean' THEN 'BOOLEAN'
                    WHEN data_type = 'numeric' THEN 'NUMERIC'
                    WHEN data_type = 'character varying' OR data_type = 'text' THEN 'STRING'
                    WHEN data_type = 'jsonb' THEN 'STRING'
                    ELSE 'STRING'
                END AS type 
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE table_name = '{table_name}' and table_catalog = '{db_name}' """

    # --- Combine query with UNION ALL for created_time_etl and modified_time_etl
    request_full = f""" UNION ALL
                SELECT 'NULLABLE' AS mode, 'created_time_etl' AS name, 'TIMESTAMP' AS type
                UNION ALL
                SELECT 'NULLABLE' AS mode, 'modified_time_etl' AS name, 'TIMESTAMP' AS type
    """

    # --- Stored query based on ingestion type
    if(ingest_type == "full"):
        request = request + request_full
    
    # --- Hook the connection to process and stored the query
    hook       = PostgresHook(postgres_conn_id = connection)
    connection = hook.get_conn()
    df         = pd.read_sql(request, con=connection)

    return df

def get_max_check_point_time(source, connection, table, create_column, update_column, check_point_time, check_point_type = "MAX"):

    """ 
    Function for get maximum new checkpoint based on create and update column

    Params:
        source           (str) | Required: Source name on DAG
        connection       (str) | Required: Name of connection on DAG connection
        table            (str) | Required: Name of destination table
        create_column    (str) | Required: Name of create column on table source
        update_column    (str) | Required: Name of update column on table source
        check_point_time (str) | Required: Last time checkpoint from DAG Variable
        check_point_type (str) | Required: Type of checkpoint, default: "MAX"

    Return:
        New checkpoint time to set into DAG workflow

    Sample:
        get_max_check_point_time("bigquery", gcs_conn, job_destination_table, "create_date", "write_date", str(checkpoint), "MIN")
    """

    # --- Create query for get maximum new checkpoint
    if check_point_type == "MAX":
        request = f"""
            select MAX({create_column}) AS create_column, MAX({update_column}) AS update_column 
            from {table} 
            where {create_column} >= '{check_point_time}' 
            or {update_column} >= '{check_point_time}'
        """

        if(create_column == update_column):
            request = f"""
            select MAX({create_column}) AS create_column
            from {table} 
            where {create_column} >= '{check_point_time}'
            """
    elif check_point_type == "MIN":
        request = f"""
            select MIN({create_column}) AS create_column, MIN({update_column}) AS update_column 
            from {table} 
            where {create_column} >= '{check_point_time}' 
            or {update_column} >= '{check_point_time}'
        """

        if(create_column == update_column):
            request = f"""
            select MIN({create_column}) AS create_column
            from {table} 
            where {create_column} >= '{check_point_time}'
            """

    logging.info(request)

    logging.info("Create hook and execute query")

    # --- Hook the connection to stored and process the query
    if(source == 'bigquery'):
        hook      = BigQueryHook(bigquery_conn_id = connection, use_legacy_sql = False)
        bq_client = bigquery.Client(project = hook._get_field("project"), credentials = hook._get_credentials())
        df        = bq_client.query(request).to_dataframe()
    else:
        hook       = PostgresHook(postgres_conn_id = connection)
        connection = hook.get_conn()
        df         = pd.read_sql(request, con=connection)
    
    # --- Stored the new checkpoint into variable
    create_column_result = df["create_column"].iloc[0]
    check_point_time_return = create_column_result

    if(create_column != update_column):
        update_column_result = df["update_column"].iloc[0]
        
        if update_column_result < create_column_result:
            check_point_time_return = update_column_result
    
    return check_point_time_return

def construct_merge(column_list):

    """
    Function for merge or update the query from temp dataset to dataset
    This function is to merging query specificaly for schema that stored in JSON

    Params: 
        column_list (list) | Required : List that contain name of column on table to be merging

    Return:
        Query for merging name of column in column list -> Need to find query on XCOM

    Sample:
        construct_merge(column_list)
    """

    final_list = [] # --- For storing

    try:
        if column_list[0] == '-': return ''
    except:
        return ''

    # --- Merging the field's based on their values
    for i in range(len(column_list)):
        astring = 'T.' + column_list[i] + '=' + 'S.' + column_list[i]
        final_list.append(astring)

    # --- Combine all the field's into variable
    final_string = ','.join(final_list)

    return final_string


def string_to_date(input_date):

    """
    Function to change string into date

    Params:
        input_date (str) | Required : String that contain date on processing data BigQuery

    Return:
        Date string with data type "DATE" 

    Sample:
        string_to_date(2021-12-23)
    """

    try:
        day,month,year = input_date.split('/') # --- Split the string based on '/'
        return year+"-"+month+"-"+day # --- Stored the date
    except:
        return input_date

def timestamp_to_date(input_date:str) -> str:

    """
    Function for change data type from timsetamp into date

    Params:
        input_date (str) | Required : Input date with data type timestamp that change into string automaticaly

    Return:
        Input date with data type "DATE"

    Sample:
        timestamp_to_date(2021-12-23 17:21:14) -> <Need to change into the real string>
    """

    try:
        timestamp = float(input_date)
        timestamp = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S') # --- Change the format timestamp
    except:
        timestamp = input_date
    finally:
        return timestamp

def json_read_file(file_json_path):

    """
    Function to read records on json file

    Params:
        file_json_path (str) | Required : Destination path for file JSON

    Return:
        None (Read file JSON)

    Sample:
        json_read_file(file_json_path=job_configuration_file_path)
    """

    json_value = {}
    try:
        with open(file_json_path) as job_disbursement:
            json_value = json.load(job_disbursement) # --- Read JSON File
            job_disbursement.close()
            log_info = "Finish reading file: " + file_json_path
        logging.info(log_info)
    except Exception as e:
        logging.exception('Error while reading the data: ', e)
    return json_value    

def listing_folders_in_gcs(gcs_bucket_path, prefix):

    """
    Function to reading Folder on Bucket

    Params:
        gcs_bucket_path (str) | Required : Path of GCS bucket
        prefix (str) | Required : Type of prefix

    Return:
        None (Reading Folder)

    Sample:
        listing_folders_in_gcs('sirclo-1152-analytics-temp/promo_plan', prefix = '') 
    """

    command = ['gsutil','ls','gs://' + gcs_bucket_path + '/' + prefix]

    # --- Combine and capture both streams into one for spawn the new process
    list_file_in_gcs = subprocess.run(command, stdout = subprocess.PIPE, stderr = subprocess.STDOUT)

    # --- Decoding into utf-8
    list_file_in_gcs = list_file_in_gcs.stdout.decode('utf-8')

    return list_file_in_gcs

def listing_file_in_gcs_folder(gcs_bucket_path, prefix, ext='csv'):

    """
    Function to reading file on Bucket Folder

    Params:
        gcs_bucket_path (str)       | Required : Path file of GCS bucket
        prefix          (str)       | Required : Type of prefix
        ext             (str: csv)  | Optional : Extention of file

    Return:
        None (Reading File)

    Sample:
        listing_file_in_gcs_folder(gcs_bucket_path = 'sirclo-1152-analytics-temp/promo_plan', prefix = '')    
    """
    # --- Read the file inside the folder in GCS BUCKET
    list_file_in_gcs = listing_folders_in_gcs(gcs_bucket_path, prefix)

    # Find and stored file with .csv type
    list_file_in_gcs = (re.findall(r'\w+\.' + ext, list_file_in_gcs))

    return list_file_in_gcs

def change_case(str):

    """
    Function for change special characters (non alphabetical and numerical) into none
    And stored it into string

    Params:
        str (str) | Required : Name of files on strings which contain special characters

    Return:
        Clean file name without special character

    Sample:
        change_case(folder_name)
    """
    # --- Describe and change every special characters into sting
    str = re.sub('[^A-Za-z0-9 ]+', '', str)

    # --- Describe type file format into parting file
    s1  = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', str)

    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()
 
def convert_fields_to_string(table_bigquery_schema):

    """
    Function for converting any field's type on table into String

    Params:
        table_bigquery_schema (dataframe) | Required : Dataframe from GCS that contain schema of table
    
    Return:
        Outuput data that contain schema table with field's type is STRING

    Sample:
        convert_fields_to_string(table_bigquery_schema = shipping_job_configuration['table_bigquery_target_schema'])
    """
    
    # --- Stored the table
    input_data = table_bigquery_schema

    # --- Change type in table into STRING
    for item in input_data:
        item['type'] = 'STRING'

    output_data = input_data # --- Stored the result

    return output_data

def query_to_list(fetch_datas):

    """
    Function for store query file into list

    Params:
        fetch_datas (query) | Required : Query from SQL file that contain important base file

    Return:
        List that contain query for stored into excel

    Sample:
        query_to_list(sources)
    """

    new_fetch_datas = []

    # --- Stored query from SQL
    for fetch_data in fetch_datas:
        new_data = []

        for data in fetch_data:

            # --- Fetch every datas
            if data == None:
                data = ""
            elif type(data) == int:
                data = data
            else:
                data = str(data)

            # --- Stored query's into list
            new_data.append(data)
        new_fetch_datas.append(new_data)
    
    return new_fetch_datas

def check_record_exist(connection, type_conn, min_record, batch_limit, table):

    """
    Function for check existed record data type on table

    Params:
        connection (str) | Required : Name of connection based on table in DAG connection
        type_conn (str) | Required : Type of connection (MySQL or Postgres)
        min_record (str/int) | Required : Name of id 
        batch_limit (int) | Required : Name of batch limit of row on table
        table (str) | Required : Name of table

    Return:
        List that contain the next row of a query result set and returns a single sequence, or None

    Sample:
        check_record_exist('pg_odoo', 'postgres', 4554, 100, 'account_invoice')
    """

    # --- Create query for get preview of data
    request = '''SELECT MAX(id) FROM (
                    SELECT
	                    id,
	                    ROW_NUMBER () OVER (ORDER BY id)
                    FROM {}
                    WHERE id > {}	
                    ORDER BY id ASC	
                    LIMIT {}) table1'''.format(table, min_record, batch_limit)

    # --- Hook the connection based on type of the connection
    if type_conn == 'postgres':
        hook = PostgresHook(postgres_conn_id=connection)

    elif type_conn == 'mysql':
        hook = MySqlHook(mysql_conn_id=connection)
    
    connection = hook.get_conn()
    cursor     = connection.cursor()
    cursor.execute(request)

    sources = cursor.fetchone()
    
    # --- Stored exsited or not file
    if sources[0] is not None:
        return sources[0]
    else:
        return -1


def get_limit(connection, type_conn, table, stored_value):

    """
    Function for get limit of table in any desired sources

    Params:
        connection (str) | Required : Name of connection in DAG Connection
        type_conn (str) | Required : Type of connection (MySQL or Postgres)
        table (str) | Required : Name of table
        stored_value (str) | Required : Names of value that will be stored

    Return:
         List that contain limitation id from sources

    Sample:
        get_limit('pg_odoo_13', 'postgres', 'stock_move', '90134')
    """
    # --- Get important variable and Airflow variable as config
    id_next       = 0
    batch_limit   = Variable.get('batch_limit')
    list_limit_id = []

    # --- Check existed record from connection
    while True: 
        id_next = check_record_exist(connection, type_conn, id_next, batch_limit, table)
        if(id_next < 0):
            break
        else:
            list_limit_id.append(id_next) # --- Stored the limit

    stored_value_json = {table:list_limit_id}

    # --- Get line from specific stored value
    if stored_value != 'None':
        stored_value      = stored_value.replace("'", '"')
        stored_value_json = json.loads(stored_value)
        stored_value_json[table] = list_limit_id

    return stored_value_json


def get_table_schema_from_gcs(gcs_conn, bucket, table, schema_filename, temp_dir):

    """
    Function to download table schema from GCS to Local File

    Params:
        gcs_conn (str) | Required : Name of GCS connection
        bucket (str) | Required : Link to Google Cloud Storage Bucket
        table (str) | Required : Name of table
        schema_filename (str) | Required : Name of path for schema file name
        temp_dir (str) | Required: Name of directory path for local temp

    Return:
        None (Done downloading table schema from GCS)

    Sample:
        get_table_schema_from_gcs(gcs_conn="gcs_prod", bucket='sirclo-1152-analytics-temp', table='stock_move_line', schema_filename='commerce_to_bq/odoo13/live_update/', temp_dir="/tmp/odoo_13/")
    """

    # --- Get important configuration
    table           = table
    schema_filename = schema_filename

    temp_dir  = temp_dir
    temp_path = temp_dir + table + ".json"

    # --- Get or create temp directory
    if not os.path.exists(temp_dir): 
        os.makedirs(temp_dir)

    # --- Hook the Google Cloud Storage connection
    gcs_hook   = GoogleCloudStorageHook(gcp_conn_id=gcs_conn)
    gcs_client = gcs_hook.get_conn()
    gcs_bucket = gcs_client.bucket(bucket)
    blob       = gcs_bucket.blob(schema_filename)

    try:
        # --- Download extracted schema from GCS to be validated
        logging.info(schema_filename)
        blob.download_to_filename(temp_path)
    except:
        logging.info()

def upload_to_gcs(conn: str, bucket: str, bucket_path: str, bucket_filename: str, dataframe: pd.DataFrame = None, filetype: str = ".json", tmp_path: str = "/tmp/", tmp_filename: str = None, use_bucket_path_as_tmp_path: bool = True, lines: bool = True, privacy: str = "private", remove_tmp_file: bool = True, delimiter: str = "|", quotechar: str = '"'):
    """
    Function for upload to GCS with temporary file method

    Params: 
        conn                        (str)            : gcs connection
        bucket                      (str)            : gcs bucket
        bucket_path                 (str)            : gcs bucket path, so full bucket path is `bucket/bucket_path`
        bucket_filename             (str)            : gcs file name,   so full file path is `bucket/bucket_path/bucket_filename`. if `tmp_filename` is None then `tmp_filename` is `bucket_filename`
        dataframe                   (dataframe: None): dataframe that want to be uploaded. if None then no save temporary file and the method will upload existing file (make sure the file is exist)
        filetype                    (str: .json)     : file type of temporary file and uploaded file
        tmp_path                    (str: /tmp/)     : temporary directory path. system will make the directory if the path doesn't exists
        tmp_filename                (str: None)      : temporary filename. if None then `tmp_filename` is `bucket_filename`
        use_bucket_path_as_tmp_path (bool: True)     : if True then `bucket` and `bucket_path` will be path of temporary file in order to uniqueness filepath, so full temporary path is `tmp_path/bucket/bucket_path/`
        lines                       (bool: True)     : the file will be writen out line-delimited json format.
        remove_tmp_file             (bool: True)     : the file will be deleted after upload.
        privacy                     (str)            : the file will be made public or private, depending on the value.
        delimiter                   (str)            : only used for .csv format, to specify the field delimiter
        quotechar                	(str)            : only used for .csv format, to specify the field quotechar

    Returns:
        public_url (str): url of gcs filepath

    Sample:
        upload_to_gcs('gcs_prod', 'sirclo-analytics-temp', 'commerce_to_bq/odoo_fat_new/account_journal/', account_journal + "_" + 'odoo11' + "_"+str(0), transformed_df, ".gz")
    """

    # --- Initialize connection
    gcs_hook   = GoogleCloudStorageHook(gcp_conn_id=conn)
    gcs_client = gcs_hook.get_conn()
    gcs_bucket = gcs_client.bucket(bucket)

    # --- Check folder names
    if use_bucket_path_as_tmp_path:
        tmp_path = os.path.join(tmp_path, bucket, bucket_path)
    
    if not os.path.exists(tmp_path):
        os.makedirs(tmp_path)

    # --- Set upload environtment
    if tmp_filename is None:
        tmp_filename = bucket_filename

    bucket_filename = bucket_filename + filetype
    bucket_filepath = os.path.join(bucket_path, bucket_filename)
    tmp_filename    = tmp_filename + filetype
    tmp_filepath    = os.path.join(tmp_path, tmp_filename)

    # --- Set where to upload to gcs
    blob = gcs_bucket.blob(bucket_filepath)
    
    # --- Condition filetype (.gz, .json, .csv)
    if dataframe is not None:
        if filetype == ".gz":
            dataframe.to_json(tmp_filepath, orient='records', lines=lines, force_ascii=False, date_format='iso', compression='gzip')
        elif filetype == ".json":
            dataframe.to_json(tmp_filepath, orient='records', lines=lines, force_ascii=False, date_format='iso')
        elif filetype == ".csv":
            dataframe.to_csv(tmp_filepath, sep=delimiter, quotechar=quotechar, index=False)
        elif filetype == ".parquet":
            dataframe.to_parquet(tmp_filepath)

    # --- Upload to bucket
    blob.upload_from_filename(tmp_filepath)
    print("Finish Upload File to Bucket: " + bucket_filepath)
    
    # --- Del temporary file
    if remove_tmp_file:
        os.remove(tmp_filepath)

    if(privacy != "private"):
        blob.make_public()
        
    return blob.public_url

def list_of_files_in_gcs(gcs_conn, bucket, prefix=None):

    """
    Function for check if table exist in BigQuery Bucket

    Params:
        gcs_conn (str) | Required : Name of Google Cloud Connection
        bucket (str) | Required : Link to Google Cloud Storage Bucket
        prefix (str) | Required : Prefix address

    Return:
        List that contain file address

    Sample:
        list_of_files_in_gcs('gcs_conn', 'sirclo-analytics-temp', 'crawler/tokopedia/<today_date>')
    """

    # --- Hook the Google Cloud Storage Connection
    gcs_hook   = GoogleCloudStorageHook(gcp_conn_id=gcs_conn)
    gcs_client = gcs_hook.get_conn()
    blobs      = gcs_client.list_blobs(bucket, prefix=prefix)
    
    query = []

    for counter, blob in enumerate(blobs):
        if counter != 0:
            break

        query = blob.download_as_string().decode('utf-8') # --- Download as string file in gcs and decode it into utf-8
        query = query.split(';\n')
    
    return query

def listing_date_file_gcs(gcs_bucket_path, prefix):

    """
    Function for reading file on Bucket Folder

    Params:
        gcs_bucket_path (str) | Required: Link to Google Cloud Storage Bucket
        prefix          (str) | Required: Prefix address

    Return:
        List of date from bucket file

    Sample:
        listing_date_file_gcs('sirclo-analytics-shipping', folder_name)

    """
    # --- Get listing file in GCS
    list_file_in_gcs = listing_folders_in_gcs(gcs_bucket_path, prefix)

    # --- Get date list from stored file
    list_date        = (re.findall(r'\d+', list_file_in_gcs))

    return list_date

def get_column_query(listnya):

    """
    Function for get list of field's into string for SELECT purposes in query

    Params:
        listnya (list) | Required : Name of SKU's to be stored as field in table

    Return:
        A string that contain multiple fields for query purposes

    Sample:
        get_column_query(['jne', 'bukalapak'])
    """

    query = []

    for item in listnya:
        column = f'validation.{item["name"]}.is_invalid as {item["name"]}' # --- Create the query for exchange purposes
        query.append(column) # --- Stored the query

    return(', '.join(query))

def unix2human(unixtime, fmt = "%Y-%m-%d"):

    """
    Function for check date file in desired folder

    Params:
        unixtime (datetime) | Required : Date or Time of last modification on folder
        fmt (regex) | Required : Type of datetime
    
    Return:
        Time of last modification on folder with specific type

    Sample:
        unix2human(filename.st_mtime)
    """

    try:
        return datetime.utcfromtimestamp(int(unixtime)).strftime(fmt) # --- Stored unixtime string

    except Exception as e:
        logging.error("Failed to convert unixtime string '{}' :: {}".format(unixtime, e))
        return None

def get_query_from_file(path, filename, vars={}) -> str:
    """
    Function for get query from .sql file in order to make simply query call

    Params:
        path     (str) | Required : Query path
        filename (str) | Required : Query filename
        vars     (str) | Required : Variable in file inner `{key}` and replace with value

    Returns:
        Query string with replaced variable

    Sample:
        get_query_from_file(resources_path + 'utility', "get_schema_mysql.sql", {"table": <shopconfig>, "database": <common-info>})
    """

    with open(os.path.join(path, filename), "r") as f:
        datas = f.read() # --- Read the file

        for key, value in vars.items():
            datas = datas.replace(f"{{{key}}}", value) # --- Stored the content from the file
        f.close()

    return datas

def transform_dtype(dataframe: pd.DataFrame, schema: list) -> pd.DataFrame:
    """
    Function for transorm data type of dataframe based on BigQuery schema

    Params:
        dataframe   (dataframe) | Required : The dataframe
        schema      (list) 		| Required : Schema of table bigquery

    Returns:
        Dataframe with the correct data type

    Sample:
        transform_dtype(df, schema)
    """
    
    # --- Stored the field's and their type of data that contained
    if not dataframe.empty:
        for key in schema:
            name  = key["name"]
            dtype = key["type"]

            # --- For time and date purposes
            if "TIME" in dtype or "DATE" in dtype:
                dataframe[name] = dataframe[name].astype("str")
                dataframe[name] = dataframe[name].apply(lambda x: pd.to_datetime(x, errors="coerce", utc=True))

                if dtype == "DATE":
                    dataframe[name] = dataframe[name].dt.date
                    dataframe[name] = dataframe[name].replace({pd.NaT: None})

                elif dtype == "TIME":
                    dataframe[name] = dataframe[name].dt.time

            elif dtype == "INTEGER":
                dataframe[name] = dataframe[name].astype("Int64") # --- For integer purposes
            elif dtype == "FLOAT":
                dataframe[name] = dataframe[name].astype("float") # --- For float purposes
            elif dtype == "STRING":
                dataframe[name] = dataframe[name].apply(lambda x: str(x) if x is not None and x == x else x) # --- For string purposes
            elif dtype == "BOOLEAN":
                dataframe[name] = dataframe[name].astype("boolean") # --- For boolean purposes

    return dataframe

def parse_type(schema_type: str) -> str:
    """
    Function for parse the schema type to pandas type in order to specifying dataframe type

    Params:
        schema_type (str) | Required : Schema type like int64, float64, char, ...

    Returns:
        Type changed like TIMESTAMP, FLOAT, INTEGER, ...

    Sample:
        lambda dtype: parse_type(dtype) -> Usually used as lambda function
    """

    # --- Type of parsing
    conf = {
        "datetime" : "TIMESTAMP",
        "timestamp": "TIMESTAMP",
        "bool"     : "BOOLEAN",
        "int"      : "INTEGER",
        "float"    : "FLOAT",
        "numeric"  : "FLOAT",
        "double"   : "FLOAT",
        "decimal"  : "FLOAT",
        "time"     : "TIME",
        "date"     : "DATE",
    }

    # --- Stored and exchange for specific type and their parsing
    for key, values in conf.items():
        if key in schema_type:
            return values
    
    return "STRING"


def job_configuration(schema, append, partition_by=None, clusters=[]) -> bigquery.LoadJobConfig:
    """
    Function for job configuration for load to bq

    Params:
        schema          (dict) | Required   : Schema of current table
        append          (bool) | Required   : If true, write disposition will be WRITE_APPEND, else WRITE_TRUNCATE
        partition_by    (str)  | Required   : Field want to be partition field (None)
        clusters        (list) | Required   : List of clusters ([])

    Returns:
        Job configuration

    Sample:
        job_configuration
    """

    # --- Stored the configuration
    config = {
        "schema"           : schema,
        "write_disposition": "WRITE_APPEND" if append else "WRITE_TRUNCATE"
    }

    # --- Clustered all the partition based on their schema
    if len(clusters) > 0:
        config["clustering_fields"] = clusters
    
    if partition_by is not None:
        config["time_partitioning"] = bigquery.TimePartitioning(
            type_                    = "DAY",
            field                    = partition_by,
            require_partition_filter = False
        )

    return bigquery.LoadJobConfig(**config)

def check_file_exists_in_gcs(gcs_conn: str, bucket: str, bucket_filepath: str) -> bool:
    """
    Function for check if the file exist in GCS Bucket

    Params:
        gcs_conn        (str) | Required : Name of Google Cloud Connection
        bucket          (str) | Required : Link to Google Cloud Storage Bucket
        bucket_filepath (str) | Required : Filepath in Google Cloud Storage

    Return:
        does the file exists

    Sample:
        check_file_exists_in_gcs('gcs_conn', 'sirclo-analytics-temp', 'crawler/tokopedia/batch-1.parquet')
    """

    # --- Hook the Google Cloud Storage Connection
    gcs_hook    = GoogleCloudStorageHook(gcp_conn_id=gcs_conn)
    gcs_client  = gcs_hook.get_conn()
    gcs_bucket  = gcs_client.bucket(bucket_name=bucket)
    file_exists = storage.Blob(name=bucket_filepath, bucket=gcs_bucket).exists()
    
    return file_exists

def read_file_buffer_from_gcs(gcs_conn: str, bucket: str, bucket_filepath: str) -> io.BytesIO:
    """
    Function for get buffer data from specific file of GCS

    Params:
        gcs_conn        (str) | Required : Name of Google Cloud Connection
        bucket          (str) | Required : Link to Google Cloud Storage Bucket
        bucket_filepath (str) | Required : Filepath in Google Cloud Storage

    Return:
        Buffer data

    Sample:
        pd.read_parquet(read_file_buffer_from_gcs('gcs_conn', 'sirclo-analytics-temp', 'crawler/tokopedia/batch-1.parquet'))
    """

    # --- Hook the Google Cloud Storage Connection
    gcs_hook   = GoogleCloudStorageHook(gcp_conn_id=gcs_conn)
    gcs_client = gcs_hook.get_conn()
    gcs_bucket = gcs_client.bucket(bucket_name=bucket)
    file_blob  = gcs_bucket.blob(bucket_filepath)
    file_data  = file_blob.download_as_string()
    return io.BytesIO(file_data)
    
def map_hubspot_properties(properties: list, key: str, value: str) -> dict:
    """
    Function for mapping between hubspot porperty name (id) and label, vice versa

    Params:
        properties  (str) | Required : property data of hubspot
        key         (str) | Required : key property to become key on the result (only 'name' | 'label')
        value       (str) | Required : key property to become value on the result (only 'name' | 'label')

    Return:
        map of key and value ({name: label} or {label: name})

    Sample:
        map_hubspot_properties(properties['results'], 'name', 'label')
    """

    # validat key and value param
    assert key in ['name', 'label']
    assert value in ['name', 'label']

    # convert case to become 'bq column name able'
    snake_case       = lambda data: re.sub(r'[^A-Za-z0-9]', '_', data.lower())
    handle_num_first = lambda data: '_' + data if re.match(r'^\d', data) else data
    convert_prop     = lambda prop, index: prop[index] if index == 'name' else handle_num_first(snake_case(prop[index]))
    
    return {convert_prop(prop, key): convert_prop(prop, value) for prop in properties}

def trigger_dbt_job_until_complete(token: str, account_id: str, job_id: str, cause: str, run_checking_interval: int = 60) -> tuple:
    """
    Function for trigger dbt job and will wait until run job complete

    trigger job reference : https://docs.getdbt.com/dbt-cloud/api-v2#tag/Jobs/operation/triggerRun
    get job run reference : https://docs.getdbt.com/dbt-cloud/api-v2#tag/Runs/operation/getRunById
    
    Params:
        token                   (str)     | Required : service token of the dbt
        account_id              (str)     | Required : numeric ID of the account
        job_id                  (str)     | Required : numeric ID of the job
        cause                   (str)     | Required : a text description of the reason for running this job
        run_checking_interval   (str: 60) | Optional : interval how long to wait for each run checking (seconds)

    Return:
        response of job and run (job_response, run_response)

    Sample:
        trigger_dbt_job_until_complete(account_id='13443', job_id='110680', token='123', cause='trigger job from airflow')
    """

    # trigger job
    run_response = None
    job_response = requests.post(
        url     = f'https://cloud.getdbt.com/api/v2/accounts/{account_id}/jobs/{job_id}/run/',
        headers = {'Content-Type' : 'application/json', 'Authorization': f'Token {token}'},
        json    = {'cause' : cause}
    ).json()

    print(job_response['data']['href'])
    print('running...')

    # check is current job completed
    while True:
        time.sleep(run_checking_interval)
        
        run_response = requests.get(
            url     = f'https://cloud.getdbt.com/api/v2/accounts/{account_id}/runs/{job_response["data"]["id"]}/',
            headers = {'Content-Type' : 'application/json', 'Authorization': f'Token {token}'}
        ).json()
        
        if run_response['data']['in_progress'] == False:
            break
    
    return job_response, run_response

def psql_insert_copy(table, conn, keys, data_iter):
    """
    Execute SQL statement inserting data
    https://ellisvalentiner.com/post/a-fast-method-to-insert-a-pandas-dataframe-into-postgres/

    Parameters
    ----------
    table : pandas.io.sql.SQLTable
    conn : sqlalchemy.engine.Engine or sqlalchemy.engine.Connection
    keys : list of str
        Column names
    data_iter : Iterable that iterates the values to be inserted
    """
    # gets a DBAPI connection that can provide a cursor
    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:
        s_buf = io.StringIO()
        writer = csv.writer(s_buf)
        writer.writerows(data_iter)
        s_buf.seek(0)

        columns = ', '.join('"{}"'.format(k) for k in keys)
        if table.schema:
            table_name = '"{}"."{}"'.format(table.schema, table.name)
        else:
            table_name = table.name

        sql = 'COPY {} ({}) FROM STDIN WITH CSV'.format(
            table_name, columns)
        cur.copy_expert(sql=sql, file=s_buf)