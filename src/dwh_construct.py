from datetime import datetime, timedelta
import psycopg2

import requests
import json
import logging
import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base import BaseHook
from airflow.hooks.http_hook import HttpHook
from airflow.models.xcom import XCom
from airflow.models import Variable

# set user constants for api
NICKNAME = Variable.get('X-Nickname')
COHORT = Variable.get('X-Cohort')
http_conn_rest = HttpHook.get_connection('http_connection_restaurants')
http_conn_cour = HttpHook.get_connection('http_connection_couriers')
http_conn_del = HttpHook.get_connection('http_connection_deliveries')
api_key = Variable.get('X-API-KEY_secret')
base_url_rest = http_conn_rest.host
base_url_cour = http_conn_cour.host
base_url_del = http_conn_del.host

headers = {
    "X-API-KEY": api_key,
    "X-Nickname": NICKNAME,
    "X-Cohort": COHORT
}

task_logger = logging.getLogger("airflow.task")

conn_id = 'PG_WAREHOUSE_CONNECTION'
dwh_hook = PostgresHook(conn_id)
conn = dwh_hook.get_conn()
cur = conn.cursor()

def extract_restaurants_data(ti,base_url_rest, headers):
    task_logger.info(f"URL: {base_url_rest}")
    response = requests.get(f'{base_url_rest}?sort_field=restaurant_id&sort_direction=asc&limit=50&offset=0',headers=headers)
    response_dict = json.loads(response.content)
    for offset_step in range(50,350, 50):
        response = requests.get(f'{base_url_rest}?sort_field=restaurant_id&sort_direction=asc&limit=50&offset={offset_step}',headers=headers)
        response.raise_for_status()
        response_dict = response_dict + json.loads(response.content)
    task_logger.info(f'Response is {response_dict}')
    ti.xcom_push(key='restaurant_data', value=response_dict)

def extract_couriers_data(ti,base_url_cour, headers):
    response = requests.get(f'{base_url_cour}?sort_field=restaurant_id&sort_direction=asc&limit=50&offset=0',headers=headers)
    response_dict = json.loads(response.content)
    for offset_step in range(50,350, 50):
        response = requests.get(f'{base_url_cour}?sort_field=restaurant_id&sort_direction=asc&limit=50&offset={offset_step}',headers=headers)
        response.raise_for_status()
        response_dict = response_dict + json.loads(response.content)
    task_logger.info(f'Response is {response_dict}')
    ti.xcom_push(key='courier_data', value=response_dict)

def extract_deliveries_data(ti,base_url_del, headers, ts_template):
    dict = []
    flag = 0 if dict else 1
    offset = 0
    while flag:
        task_logger.warning(f'Response is {ts_template}')
        response = requests.get(f'{base_url_del}?&from={ts_template} 00:00:00&sort_field=delivery_id&sort_direction=asc&limit=50&offset={offset}',headers=headers)
        task_logger.warning(f'Response is {response.content}')
        response_dict = json.loads(response.content)
        dict = dict + response_dict
        offset += 50
        flag = 1 if response_dict else 0
    task_logger.info(f'Response is {response.content}')
    ti.xcom_push(key='delivery_data', value=dict)

def load_to_stg_restaurants(ti,cur):
    data = ti.xcom_pull(key='restaurant_data')
    cur.execute("create table if not exists stg.restaurants(id serial, restaurant_id text, object_value json, constraint restaurant_id_unique unique(restaurant_id));")
    for each in data:
        task_logger.warning(json.dumps(each,ensure_ascii=False))
        cur.execute("""insert into stg.restaurants(restaurant_id, object_value) values(%(restaurant_id)s, %(object_value)s) 
                    on conflict (restaurant_id) do update 
                    set restaurant_id = EXCLUDED.restaurant_id;""",{"restaurant_id":each["_id"],"object_value":json.dumps(each, ensure_ascii=False)})
    conn.commit()

def load_to_stg_couriers(ti,cur):
    data = ti.xcom_pull(key='courier_data')
    cur.execute("create table if not exists stg.couriers(id serial, courier_id text, object_value json, constraint courier_id_unique unique(courier_id));")
    for each in data:
        cur.execute("insert into stg.couriers(courier_id, object_value) values(%(courier_id)s, %(object_value)s) on conflict (courier_id) do update set courier_id=EXCLUDED.courier_id;",{"courier_id":each["_id"],"object_value":json.dumps(each, ensure_ascii=False)})
    conn.commit()

def load_to_stg_deliveries(ti,cur):
    data = ti.xcom_pull(key='delivery_data')
    cur.execute("create table if not exists stg.deliveries(id serial, delivery_id text, object_value json, update_ts timestamp, constraint delivery_id_unique unique(delivery_id));")
    for each in data:
        cur.execute("insert into stg.deliveries(delivery_id, object_value, update_ts) values(%(delivery_id)s, %(object_value)s, cast(%(update_ts)s as timestamp)) on conflict (delivery_id) do update set delivery_id=EXCLUDED.delivery_id;",{"delivery_id":each["delivery_id"],"object_value":json.dumps(each, ensure_ascii=False), "update_ts":each["order_ts"]})
    conn.commit()
    cur.close()
    conn.close()

def transform_to_dds_restaurants(cur):
    cur.execute("create table if not exists dds.restaurants(id serial primary key, restaurant_id text, restaurant_name text, constraint id_name_unique unique(restaurant_id, restaurant_name));")
    cur.execute("""insert into dds.restaurants(restaurant_id, restaurant_name) 
                select object_value ->> '_id', object_value ->> 'name' from stg.restaurants 
                on conflict (restaurant_id, restaurant_name) do update 
                    set restaurant_id = EXCLUDED.restaurant_id, 
                        restaurant_name = EXCLUDED.restaurant_name;""")
    conn.commit()

def transform_to_dds_couriers(cur):
    cur.execute("create table if not exists dds.couriers(id serial primary key, courier_id text, courier_name text, constraint cour_id_name_unique unique(courier_id, courier_name));")
    cur.execute("""insert into dds.couriers(courier_id, courier_name) 
                select object_value ->> '_id', object_value ->> 'name' from stg.couriers 
                on conflict (courier_id,courier_name) do update 
                set courier_id = EXCLUDED.courier_id, 
                    courier_name = EXCLUDED.courier_name;""")
    conn.commit()

def transform_to_dds_deliveries(cur):
    # cur.execute("drop table if exists dds.deliveries cascade;")
    cur.execute("create table if not exists dds.deliveries(id serial primary key, delivery_id text, order_id text, order_ts timestamp, courier_id text, address text, delivery_ts timestamp, rate integer check (rate>=0 and rate <=5), tip_sum bigint, constraint delivery_courier_order_ids_unique unique(delivery_id, order_id, courier_id));")
    cur.execute("""insert into dds.deliveries(delivery_id, order_id, order_ts, courier_id, address, delivery_ts, rate, tip_sum) 
                select sd.delivery_id, object_value ->> 'order_id', cast(object_value ->> 'order_ts' as timestamp), object_value ->> 'courier_id', object_value ->> 'address', cast(object_value ->> 'delivery_ts' as timestamp), cast(object_value ->> 'rate' as integer), cast(object_value ->> 'tip_sum' as bigint) from stg.deliveries sd where sd.delivery_id not in (select delivery_id from dds.deliveries) -- пропускаем те доставки, которые уже загружены в dds
                on conflict (delivery_id, order_id, courier_id) do update 
                set order_id=EXCLUDED.order_id,
                    delivery_id=EXCLUDED.delivery_id,
                    courier_id=EXCLUDED.courier_id;""")
    conn.commit()
    cur.close()
    conn.close()

dag = DAG(
    dag_id='dwh_reload',
    schedule_interval='@daily',
    start_date=pendulum.datetime(2023, 10, 9, tz="UTC"),
    catchup=True,
    dagrun_timeout=timedelta(minutes=60),
    is_paused_upon_creation=True
)

extract_restaurants = PythonOperator(
    task_id='extract_restaurants',
    python_callable=extract_restaurants_data,
    op_kwargs={  
        'base_url_rest': base_url_rest,
        'headers': headers
    },
    do_xcom_push=True,
    dag=dag
)

extract_couriers = PythonOperator(
    task_id='extract_couriers',
    python_callable=extract_couriers_data,
    op_kwargs={  
        'base_url_cour': base_url_cour,
        'headers': headers
    },
    do_xcom_push=True,
    dag=dag
)

extract_deliveries = PythonOperator(
    task_id='extract_deliveries',
    python_callable=extract_deliveries_data,
    op_kwargs={  
        'base_url_del': base_url_del,
        'headers': headers,
        'ts_template': '{{ds}}'
    },
    do_xcom_push=True,
    dag=dag
)

load_restaurants = PythonOperator(
    task_id='load_to_stg_restaurants',
    python_callable=load_to_stg_restaurants,
    op_kwargs={'cur':cur},
    do_xcom_push=True,
    dag=dag
)

load_couriers = PythonOperator(
    task_id='load_to_stg_couriers',
    python_callable=load_to_stg_couriers,
    op_kwargs={'cur':cur},
    do_xcom_push=True,
    dag=dag
)

load_deliveries = PythonOperator(
    task_id='load_to_stg_deliveries',
    python_callable=load_to_stg_deliveries,
    op_kwargs={'cur':cur},
    do_xcom_push=True,
    dag=dag
)

transform_to_dds_restaurants = PythonOperator(
    task_id='transform_to_dds_restaurants',
    python_callable=transform_to_dds_restaurants,
    op_kwargs={'cur':cur},
    dag=dag
)

transform_to_dds_couriers = PythonOperator(
    task_id='transform_to_dds_couriers',
    python_callable=transform_to_dds_couriers,
    op_kwargs={'cur':cur},
    dag=dag
)

transform_to_dds_deliveries = PythonOperator(
    task_id='transform_to_dds_deliveries',
    python_callable=transform_to_dds_deliveries,
    op_kwargs={'cur':cur},
    dag=dag
)

mart_construct = PostgresOperator(
    task_id='mart_construct',
    postgres_conn_id=conn_id,
    sql="mart_construct.sql"  
)

mart_fill = PostgresOperator(
    task_id='mart_fill',
    postgres_conn_id=conn_id,
    sql="mart_fill.sql"  
)

[extract_restaurants, extract_couriers, extract_deliveries] >> load_restaurants >> load_couriers >> load_deliveries >> [transform_to_dds_restaurants,transform_to_dds_couriers,transform_to_dds_deliveries] >> mart_construct >> mart_fill
