# Docs : https://docs.google.com/document/d/1nNr4V74N7iPlWIsjmexfGIOyNGufvzpjVQtS0-fzGUI/preview

import io
import os
import sys
import json
import time
import logging
import requests
import pandas as pd
from airflow.models import Variable
from airflow import DAG, AirflowException
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from datetime import datetime, timedelta
from google.cloud import bigquery

sys.path.append(os.getcwd() + '/dags')
from helper import check_file_exists_in_gcs, map_hubspot_properties, set_slack_hook_channel, slack_hook, trigger_dbt_job_until_complete, upload_to_gcs

# -------------------- Set variable --------------------
config = Variable.get('api_to_bq_hubspot_incremental_config')
config = json.loads(config)

bq_hook   = BigQueryHook(bigquery_conn_id = config['gcp_connection'], use_legacy_sql = False)
bq_client = bigquery.Client(project = bq_hook._get_field('project'), credentials = bq_hook._get_credentials())

# -------------------- Callable of extract task --------------------
def _extract_forms(**kwargs):
    table_config     = kwargs['table_config']
    section_run_date = kwargs['section_run_date']

    # get all form properties
    req_forms_properties = requests.get(
        url = f'https://api.hubapi.com/forms/v2/forms',
        headers = {'accept': 'application/json', 'content-type': 'application/json'},
        params  = {'hapikey': config['hubspot_api_key']}
    )
    forms_properties = req_forms_properties.json()

    logging.info(f'Fetch form properties url: {req_forms_properties.url}')
    df_forms = pd.DataFrame(forms_properties)

    # convert to string if not None
    df_forms['formFieldGroups']             = df_forms['formFieldGroups'].apply(lambda x: str(x) if x is not None and x == x else x)
    df_forms['metaData']                    = df_forms['metaData'].apply(lambda x: str(x) if x is not None and x == x else x)
    df_forms['paymentSessionTemplateIds']   = df_forms['paymentSessionTemplateIds'].apply(lambda x: str(x) if x is not None and x == x else x)
    df_forms['selectedExternalOptions']     = df_forms['selectedExternalOptions'].apply(lambda x: str(x) if x is not None and x == x else x)
    
    # upload to gcs if dataframe is not empty
    if not df_forms.empty:
        df_forms['_airflow_extracted_at'] = pd.to_datetime(section_run_date)
        df_forms['_airflow_updated_at']   = pd.to_datetime(section_run_date)

        upload_to_gcs(
            conn            = config['gcp_connection'],
            bucket          = config['gcs_bucket'],
            bucket_path     = os.path.join(config['bq_dataset'], table_config['hubspot_object'], section_run_date),
            bucket_filename = f'batch-1',
            dataframe       = df_forms,
            filetype        = '.parquet',
        )

def _extract_submissions(**kwargs):
        table_config     = kwargs['table_config']
        section_run_date = kwargs['section_run_date']

        # get all form properties
        req_forms_properties = requests.get(
            url = f'https://api.hubapi.com/forms/v2/forms',
            headers = {'accept': 'application/json', 'content-type': 'application/json'},
            params  = {'hapikey': config['hubspot_api_key']}
        )
        forms_properties = req_forms_properties.json()
        logging.info(f'Fetch form properties url: {req_forms_properties.url}')

        # iterate submissions for each formID/guid
        iter = 1
        for form in forms_properties:

            # request list of questions fields for each formID/guid
            req_fields = requests.get(
                url     = f'https://api.hubapi.com/forms/v2/fields/{form["guid"]}',
                headers = {'accept': 'application/json', 'content-type': 'application/json'},
                params  = {'hapikey': config['hubspot_api_key']}
            )
            fields = req_fields.json()

            # mapping questions' internal name and displayed label
            submission_properties = map_hubspot_properties(fields, 'name', 'label')

            # loop to get all results per fromID/guid
            df_submissions = pd.DataFrame()
            after = None
            while True:
                req_submissions = requests.get(
                    url     = f'https://api.hubapi.com/form-integrations/v1/submissions/forms/{form["guid"]}',
                    headers = {'accept': 'application/json', 'content-type': 'application/json'},
                    params  = {'hapikey': config['hubspot_api_key'], 'limit': 50, 'after': after}
                )

                logging.info(f'Fetch submission url: {req_submissions.url}')
                submission = req_submissions.json()

                submission_results = submission['results']
                for result in submission_results:
                    for value in result['values']:
                        value['label'] = submission_properties.get(value['name'])

                df_submissions_temp = pd.DataFrame(submission_results)
                
                # concat results of one formId/campaign into one table
                df_submissions = pd.concat([df_submissions_temp, df_submissions], ignore_index=True)

                if not df_submissions.empty:

                    # assign 'after' url parameter if 'paging' key exists ('paging' will not exist if results have reached the end)
                    if 'paging' in submission:
                        after = submission['paging']['next']['after']
                    else:
                        
                        # assign corresponding 'formId' to table
                        df_submissions['formId'] = form["guid"]

                        # convert to string if not None
                        df_submissions['values'] = df_submissions['values'].apply(lambda x: str(x) if x is not None and x == x else x)

                        df_submissions['_airflow_extracted_at'] = pd.to_datetime(section_run_date)
                        df_submissions['_airflow_updated_at']   = pd.to_datetime(section_run_date)

                        # upload to gcs | 1 batch -> 1 formID/campaign
                        upload_to_gcs(
                            conn            = config['gcp_connection'],
                            bucket          = config['gcs_bucket'],
                            bucket_path     = os.path.join(config['bq_dataset'], table_config['hubspot_object'], section_run_date),
                            bucket_filename = f'batch-{iter}',
                            dataframe       = df_submissions,
                            filetype        = '.parquet',
                        )

                        iter += 1
                        break
                else:
                    break

def _extract_crm(**kwargs):
        table_config     = kwargs['table_config']
        section_run_date = kwargs['section_run_date']

        # get all properties
        req_all_properties = requests.get(
            url     = f'https://api.hubapi.com/crm/v3/properties/{table_config["hubspot_object"]}',
            headers = {'accept': 'application/json', 'content-type': 'application/json'},
            params  = {'hapikey': config['hubspot_api_key']}
        )
        
        logging.info(f'Fetch all properties url: {req_all_properties.url}')
        all_properties = map_hubspot_properties(req_all_properties.json()['results'], 'name', 'label')
        
        # loop for get data in batch
        last_id = 0
        iterate = 1
        while True:
            # get all data
            req_objects = requests.post(
                url     = f'https://api.hubapi.com/crm/v3/objects/{table_config["hubspot_object"]}/search',
                headers = {'accept': 'application/json', 'content-type': 'application/json'},
                params  = {'hapikey': config['hubspot_api_key']},
                json    = {
                    'limit'     : 100,
                    'properties': list(all_properties.keys()),
                    'filters': [
                        {
                            'propertyName': 'hs_object_id',
                            'operator'    : 'GT',
                            'value'       : str(last_id)
                        },
                        {
                            'propertyName': 'lastmodifieddate' if table_config['hubspot_object'] == 'contacts' else 'hs_lastmodifieddate',
                            'operator'    : 'GT',
                            'value'       : str(0 if table_config['is_full_refresh'] else int((pd.to_datetime(table_config['checkpoint']).to_pydatetime() - timedelta(minutes=10)).timestamp() * 1000))
                        },

                    ],
                    'sorts': [
                        {
                            "propertyName": "hs_object_id",
                            "direction"   : "ASCENDING"
                        }
                    ]
                }
            )
            
            logging.info(f'Fetch object url: {req_objects.url}')
            objects = req_objects.json()
            
            # if find reach limit then sleep 1 minute
            if objects.get('message') == 'You have reached your secondly limit.':
                print('sleeping...')
                time.sleep(60)
            else:
                df = pd.DataFrame(objects['results'])

                # upload to gcs if dataframe is not empty
                if not df.empty:
                    df['properties']            = df['properties'].apply(lambda props: json.dumps({value: props[key] for key, value in all_properties.items()}))
                    df['_airflow_extracted_at'] = pd.to_datetime(section_run_date)
                    df['_airflow_updated_at']   = pd.to_datetime(section_run_date)

                    upload_to_gcs(
                        conn            = config['gcp_connection'],
                        bucket          = config['gcs_bucket'],
                        bucket_path     = os.path.join(config['bq_dataset'], table_config['hubspot_object'], section_run_date),
                        bucket_filename = f'batch-{iterate}',
                        dataframe       = df,
                        filetype        = '.parquet',
                    )

                    last_id  = objects['results'][-1]['id']
                    iterate += 1
                else:
                    break
        
# -------------------- Callable of load task --------------------
def _load(**kwargs):
    table_config         = kwargs['table_config']
    section_run_date     = kwargs['section_run_date']
    bq_table_destination = kwargs['bq_table_destination']

    # load table if gcs bucket path is not empty
    if check_file_exists_in_gcs(config['gcp_connection'], config['gcs_bucket'], os.path.join(config['bq_dataset'], table_config['hubspot_object'], section_run_date, 'batch-1.parquet')):
        bq_client.load_table_from_uri(
            source_uris = 'gs://'+os.path.join(config['gcs_bucket'], config['bq_dataset'], table_config['hubspot_object'], section_run_date, '*.parquet'),
            destination = bq_table_destination,
            job_config  = bigquery.LoadJobConfig(
                autodetect         = True,
                source_format      = bigquery.SourceFormat.PARQUET,
                create_disposition = bigquery.CreateDisposition.CREATE_IF_NEEDED,
                write_disposition  = bigquery.WriteDisposition.WRITE_TRUNCATE,
            )
        ).result()
    
    logging.info(f'load to {bq_table_destination} successfully')

# -------------------- Callable of merge task --------------------
def _merge(**kwargs):
    bq_main_table = kwargs['bq_main_table']
    bq_temp_table = kwargs['bq_temp_table']

    # get main table schema
    schema = bq_client.get_table(bq_main_table).schema
    with io.StringIO("") as file: 
        bq_client.schema_to_json(schema, file)
        schema = json.loads(file.getvalue())
        file.close()

    # create query
    query = '''
        MERGE `{main_table}` T
        USING `{temp_table}` S
        ON {on_keys}
        WHEN MATCHED THEN
            UPDATE SET {merge_fields}
        WHEN NOT MATCHED THEN
            INSERT ({fields}) VALUES ({fields})
    '''.format(
            main_table   = bq_main_table,
            temp_table   = bq_temp_table,
            on_keys      = 'COALESCE(T.id, "NULL") = COALESCE(S.id, "NULL")',
            merge_fields = ', '.join([f"T.`{field['name']}` = S.`{field['name']}`" for field in schema if field['name'] != '_airflow_extracted_at']),
            fields       = ', '.join([f"`{field['name']}`" for field in schema]),
        )
    
    # execute query
    logging.info(f'Query: {query}')
    bq_client.query(query).result()

    logging.info(f'merge table successfully')

# -------------------- Callable of update_checkpoint task --------------------
def _update_checkpoint(**kwargs) -> None:
    ingest_type      = kwargs['ingest_type']
    table_config     = kwargs['table_config']
    section_run_date = kwargs['section_run_date']

    # get config of airflow variable
    config = Variable.get('api_to_bq_hubspot_incremental_config')
    config = json.loads(config)

    # get index of config by hubspot_object of current table
    is_same = lambda tbl: tbl['hubspot_object'] == table_config['hubspot_object']
    index   = next((index for (index, tbl) in enumerate(config[ingest_type + '_tables']) if is_same(tbl)), None)

    # set config with new new checkpoint
    logging.info(f'Update checkpoint. From: {table_config["checkpoint"]}. To: {section_run_date}')
    logging.info(f'Update is_full_refresh. From: {table_config["is_full_refresh"]}. To: False')
    
    config[ingest_type + '_tables'][index]['checkpoint']      = section_run_date
    config[ingest_type + '_tables'][index]['is_full_refresh'] = False
    Variable.set('api_to_bq_hubspot_incremental_config', json.dumps(config, indent=4))

# -------------------- Callable of run_dbt task --------------------
def _run_dbt(**kwargs) -> None:
    # trigger dbt job
    job_response, run_response = trigger_dbt_job_until_complete(
        token      = config['dbt_token'],
        account_id = config['dbt_account_id'],
        job_id     = config['dbt_job_id'],
        cause      = 'trigger from DAG api_to_bq_hubspot_incremental'
    )

    # if dbt job error then raise it
    if run_response['data']['is_error']:
        raise AirflowException('DBT job error')
    else:
        logging.info('DBT job success')

# -------------------- Initiate dag --------------------
set_slack_hook_channel('#data-notify-entrepreneur')
dag = DAG(
    dag_id            = 'api_to_bq_hubspot_incremental',
    catchup           = False,
    schedule_interval = config['schedule_interval'],
    max_active_runs   = 1,
    tags              = ['entrepreneur'],
    default_args      = {
        'owner'              : 'Galuh',
        'depend_on_past'     : False,
        'start_date'         : datetime(2022, 7, 25),
        'email'              : ['data.engineer@sirclo.com'],
        'email_on_failure'   : False,
        'email_on_retry'     : False,
        'concurrency'        : 1,
        'retries'            : 1,
        'retry_delay'        : timedelta(minutes=1),
        'on_failure_callback': slack_hook
    },
)

start_task = DummyOperator(task_id='start_task', dag=dag)
end_task   = DummyOperator(task_id='end_task', dag=dag)
run_dbt    = PythonOperator(
    task_id         = 'run_dbt_job',
    dag             = dag,
    python_callable = _run_dbt,
    retries         = 0,
)

# create task group
incremental_group       = TaskGroup(group_id='incremental', dag=dag)
full_unkeep_history_group = TaskGroup(group_id='full_unkeep_history', dag=dag)

# collect table config of each ingest type to 1 array
tables = []
tables.extend([('incremental', table_config) for table_config in config.get('incremental_tables', [])])
tables.extend([('full_unkeep_history', table_config) for table_config in config.get('full_unkeep_history_tables', [])])

# define extract_functions for python_callable depending on hubspot_object listed in config    
extract_function = {
    'forms'      : _extract_forms,
    'submissions': _extract_submissions,
    'crm'        : _extract_crm,
}

for ingest_type, table_config in tables:
    section_run_date    = '{{ dag_run.get_task_instance("' + ingest_type + '.' + table_config['hubspot_object'] + '__extract").start_date }}'
    task_group          = incremental_group if ingest_type == 'incremental' else full_unkeep_history_group

    bq_main_table       = f'{config["bq_dataset"]}.{table_config["bq_table_name"]}'
    bq_temp_table       = f'temp_append.{config["bq_dataset"]}__{table_config["bq_table_name"]}'


    extract = PythonOperator(
        task_id         = table_config['hubspot_object'] + '__extract',
        task_group      = task_group,
        dag             = dag,
        python_callable = extract_function['crm' if table_config['hubspot_object'] not in extract_function.keys() else table_config['hubspot_object']],
        op_kwargs       = {
            'table_config'    : table_config,
            'section_run_date': section_run_date,
        },
    )

    load = PythonOperator(
        task_id         = table_config['hubspot_object'] + '__load',
        task_group      = task_group,
        dag             = dag,
        python_callable = _load,
        op_kwargs       = {
            'table_config'        : table_config,
            'section_run_date'    : section_run_date,
            'bq_table_destination': bq_main_table if ingest_type == 'full_unkeep_history' else bq_main_table if table_config['is_full_refresh'] else bq_temp_table,
        },
    )

    # extra task for incremental type
    if ingest_type == 'incremental':

        # if not full refresh then merge it
        if table_config['is_full_refresh']:
            merge = DummyOperator(task_id=table_config['hubspot_object'] + '__merge', dag=dag, task_group=task_group)
        else:
            merge = PythonOperator(
                task_id         = table_config['hubspot_object'] + '__merge',
                task_group      = task_group,
                dag             = dag,
                python_callable = _merge,
                op_kwargs       = {
                    'bq_main_table': bq_main_table,
                    'bq_temp_table': bq_temp_table,
                },
            )

        update_checkpoint = PythonOperator(
            task_id         = table_config['hubspot_object'] + '__update_checkpoint',
            task_group      = task_group,
            dag             = dag,
            python_callable = _update_checkpoint,
            op_kwargs       = {
                'ingest_type'     : ingest_type,
                'table_config'    : table_config,
                'section_run_date': section_run_date,
            },
        )

        start_task >> extract >> load >> merge >> update_checkpoint >> run_dbt >> end_task
    else:
        start_task >> extract >> load >> run_dbt >> end_task
