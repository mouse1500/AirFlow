from io import BytesIO
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
from io import StringIO

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable

df = pd.read_csv('/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv')

sales_per_year = df.query('Year == "2009"')

default_args = {
    'owner': 'i.berezin.33',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
    'start_date': datetime(2023, 4, 19),
    'schedule_interval': '50 18 * * *'
}

@dag(default_args=default_args, catchup=False)
def global_game_sales_er_year():
   
    # Функция принимающая файл об играх.
    @task(retries=3)
    def get_data():
        top_data = sales_per_year
        return top_data
    
    # Самая продоваемая игра в этом году.
    @task(retries=4, retry_delay=timedelta(10))
    def best_selling(top_data):
        top_sells = top_data.groupby('Name', as_index=False) \
                        .agg({'Global_Sales':'count'}) \
                        .sort_values('Global_Sales', ascending=False).head(1)
        return top_sells
    
    # Самый продаваемый жанр игр в Европе.
    @task()
    def best_game_eu(top_data):
        best_game = top_data.groupby('Genre').agg({'Name':'count'}).sort_values('Name', ascending=False).head(5)
        return best_game.to_csv(index=False)

    # На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке?
    @task()
    def best_NA_platform(top_data):
        best_NA = top_data.groupby('Platform').agg({'Global_Sales':'count'}) \
                      .sort_values('Global_Sales', ascending=False).head(1)
        return best_NA.to_csv(index=False)
    
    #  У какого издателя самые высокие средние продажи в Японии?
    @task()
    def sales_in_japan(top_data):
        sales_mean_top_5 = top_data.groupby('Platform', as_index=False) \
                  .agg({'JP_Sales' : 'mean'}) \
                  .sort_values('JP_Sales', ascending=False).head(5)
        return sales_mean_top_5.to_csv(index=False)
    
    # Сколько игр продались лучше в Европе, чем в Японии?
    @task()
    def comparison_of_sales_by_EU_and_JP(top_data):
        sales_table = top_data.groupby('Name', as_index=False) \
                         .agg({'EU_Sales' : 'sum', 'JP_Sales' : 'sum'}) \
                         .sort_values('EU_Sales', ascending=False)
   
        comparison = [sales_table['EU_Sales'] > sales_table['JP_Sales'], sales_table['EU_Sales'] < sales_table['JP_Sales']]
        choices = ['A', 'B']
        sales_table['winner'] = np.select(comparison, choices, default='Tie')
        sales_EU = sales_table.query('winner == "A"')['EU_Sales'].count()
    
        return sales_EU
    
    #  Финальный таск который собирает все ответы.
    @task()
    def print_all_data(task_1, task_2, task_3, task_4, task_5):
    
        context = get_current_context()
        date = context['ds']
    
        top_sells = task_1['top_sells']
        best_game = task_2['best_game']
        best_NA = task_3['best_NA']
        sales_mean_top_5 = task_4['sales_mean_top_5']
        sales_EU = task_5['sales_EU']
        
        print(f'''Самая популярная игра года {top_sells}
                  Самые продоваемые игры EU {best_game}
                  Самая популярная платформа NA {best_NA}
                  Самые высокие средние продажи по JP {sales_mean_top_5}
                  Самые большие продажи в EU в сравнеии с JP {sales_EU}
                  ''')
   

    top_data = get_data()
    top_sells = best_selling(top_data)
    best_game = best_game_eu(top_data)
    best_NA = best_NA_platform(top_data)
    sales_mean_top_5 = sales_in_japan(top_data)
    sales_EU = comparison_of_sales_by_EU_and_JP(top_data)
    
    print_all_data(top_sells, best_game, best_NA, sales_mean_top_5, sales_EU)
    
global_game_sales_er_year = global_game_sales_er_year()