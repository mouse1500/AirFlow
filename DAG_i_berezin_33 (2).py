import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'


def get_data():
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)

def len_domain():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    df['low'] = df['domain'].str.len()
    top_1 = df.sort_values(by=['low', 'domain'], ascending=False).reset_index(drop=True).head(1)    

    with open('top_1.csv', 'w') as f:
        f.write(top_1.to_csv(index=False, header=False))

def top_10_domain():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    df['domain_zone'] = df.domain.str.split('.', n=1, expand=True).rename(columns={0 : 'domain_name', 1 : 'zone'}).drop(columns=['domain_name'])
    top_10_domain = df.domain_zone.value_counts().head(10)
    
    with open('top_10_domain.csv', 'w') as f:
        f.write(top_10_domain.to_csv(index=False, header=False))

def rank_flow():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    rank_airflow = df.query('domain == "airflow.com"')['rank']
    
    with open('rank_airflow.csv', 'w') as f:
        f.write(rank_airflow.to_csv(index=False, header=False))
        
def print_domain(ds):
    with open('top_1.csv', 'r') as f:
        top_1_data = f.read()
    
    with open('top_10_domain.csv', 'r') as f:
        top_10_data = f.read()
    
    with open('rank_airflow.csv', 'r') as f:
        rank_data = f.read()
    
    date = ds
    
    print(f'Top 1 domain by length {date}')
    print(top_1_data)

    print(f'Top 10 domain zones {date}')
    print(top_10_data)
    
    print(f'Rank Airflow {date}')
    print(rank_data)


default_args = {
    'owner': 'a.batalov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=7),
    'start_date': datetime(2023, 4, 18),
}
schedule_interval = '35 9 * * *'

dag_i_berezin_33 = DAG('top_domain_i_berezin_33', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag_i_berezin_33)

t2 = PythonOperator(task_id='len_domain',
                    python_callable=len_domain,
                    dag=dag_i_berezin_33)

t2_top_10 =  PythonOperator(task_id='top_10_domain',
                    python_callable=top_10_domain,
                    dag=dag_i_berezin_33)

t2_rank = PythonOperator(task_id='rank_flow',
                    python_callable=rank_flow,
                    dag=dag_i_berezin_33)

t3 = PythonOperator(task_id='print_domain',
                    python_callable=print_domain,
                    dag=dag_i_berezin_33)

t1 >> [t2, t2_top_10, t2_rank] >> t3