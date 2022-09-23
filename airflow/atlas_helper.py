from datetime import datetime
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
import time, pytz, numpy, logging, math

__slack_hook_channel_atlas = "#atlas-monitoring"

def slack_hook_success(**context):
    global __slack_hook_channel_atlas
    alert_color = "#42ba96"
    slack_msg = "Task Success* on DAG: *" + context['task_instance'].dag_id + "*"

    slack_attachment = [
        {
            "mrkdwn_in": ["text"],
            "color": alert_color,
            "fields": [
                {
                    "title": "Scheduled: ",
                    "value": str(datetime.fromtimestamp(context['task_instance'].start_date.timestamp(),
                                                        tz=pytz.timezone("Asia/Jakarta")).strftime('%Y-%m-%d %H:%M:%S')),
                    "short": True
                },
            ],
            "footer": "Airflow Slack API",
            "footer_icon": "https://img2.pngio.com/apache-airflow-documentation-airflow-documentation-airflow-png-700_700.png",
            "ts": str(time.time())
        }
    ]

    slack_hook =  SlackWebhookOperator(
        task_id='atlas_success_slack_hook',
        http_conn_id='atlas_slack_connection',
        message=slack_msg,
        attachments=slack_attachment,
        channel=__slack_hook_channel_atlas,
        username='airflow_local',
        icon_emoji=None,
        link_names=False,
        dag=context['dag_run'].dag)

    return slack_hook.execute(context=context)

def handle_callable():
    return True

def generate_query_with_parameters_from_config(**kwargs):
    raw_sql = kwargs['raw_sql']
    sql_with_filter_brand_id = kwargs['sql_with_filter_brand_id']

    brand_ids = kwargs['dag_run'].conf.get('brand_ids', [])
    start_date = kwargs['dag_run'].conf.get('start_date', kwargs['start_date'])
    end_date = kwargs['dag_run'].conf.get('end_date', kwargs['end_date'])

    brand_ids_query = ''
    if len(brand_ids) != 0:
        brand_ids_query = sql_with_filter_brand_id.format(brand_ids = "', '".join(brand_ids))

    sql_query = raw_sql.format(sql_with_filter_brand_id = brand_ids_query,
                               start_date = start_date,
                               end_date = end_date)

    return sql_query

def construct_merge(column_list):
    final_list = []

    for i in range(len(column_list)):
        astring = 'T.' + column_list[i] + '=' + 'S.' + column_list[i]
        final_list.append(astring)

    final_string = ','.join(final_list)

    return final_string

def construct_values(data_frame, report_db_schema):
    data_records = data_frame.to_dict('records')

    formatted_data_rows = []
    for data_record in data_records:
        i = 0
        formatted_data_columns = []

        for key in data_record:
            data_string = str(data_record[key])

            if data_record[key] != None:
                data_type = report_db_schema[key]
                i += 1

                if data_type == 'timestamp':
                    # if not NaT
                    if data_string is not None and data_string == data_string:
                        formatted_data_columns.append(f"to_timestamp('{data_string}', 'YYYY-MM-DD HH24:MI:SS')")
                    else:
                        formatted_data_columns.append('null')
                elif data_type == 'money':
                    money_value = list(data_record[key].values())

                    if money_value[0] != None and money_value[1] != None:
                        formatted_data_columns.append(f"('{money_value[0]}', {str(money_value[1])})")
                    else:
                        formatted_data_columns.append('null')
                elif data_type == 'integer':
                    if math.isnan(data_record[key]):
                        formatted_data_columns.append('0')
                    else:
                        formatted_data_columns.append(data_string)
                else:
                    formatted_data_columns.append(f"$q${data_string}$q$")
            else:
                formatted_data_columns.append('null')

        formatted_data_rows.append("(" + (", ".join(formatted_data_columns)) + ")")

    return ", ".join(formatted_data_rows)

def slack_hook_failure(context):
    global __slack_hook_channel_atlas
    slack_msg="*Warning! Task Retry* on DAG: *" + context.get('task_instance').dag_id + "*"

    alert_color = "#FFFF00"
    alert_value = "Medium!"

    # allow for maximum no retry
    if context.get('task_instance')._try_number <= 1 and context.get('task_instance').state != "failed" :
        return

    # different alert style for retry and failed task
    if context.get('task_instance').state == "failed":
        alert_color = "#FF0000"
        alert_value = "High!!"
        slack_msg = "*Attention!! Task Failed* on DAG: *" + context.get('task_instance').dag_id + "*"

    err_msg = str(context['exception'])
    final_msg = (err_msg[:140] + '...') if len(err_msg) > 140 else err_msg

    slack_attachment = [
        {
            "mrkdwn_in": ["text"],
            "color": alert_color,
            "fields": [
                {
                    "title": "Task Id: ",
                    "value" : context.get('task_instance').task_id,
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
            "footer": "Airflow Slack API",
            "footer_icon": "https://img2.pngio.com/apache-airflow-documentation-airflow-documentation-airflow-png-700_700.png",
            "ts": str(time.time())
        }
    ]

    slack_hook =  SlackWebhookOperator(
        task_id='atlas_failed_slack_hook',
        http_conn_id='atlas_slack_connection',
        message=slack_msg,
        attachments=slack_attachment,
        channel=__slack_hook_channel_atlas,
        username='airflow_local',
        icon_emoji=None,
        link_names=False,
        dag=context.get('dag_run').dag)

    return slack_hook.execute(context=context)