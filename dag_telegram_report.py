from airflow.decorators  import dag, task
from airflow.operators.python import get_current_context
import telegram
from telegram.utils.request import Request 
import pandas as pd
import pandahouse as ph
import io
import seaborn as sns
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
from datetime import datetime, timedelta

import os
from dotenv import load_dotenv

load_dotenv()

# для загрузки отчета в чат через бота в телеграм
my_token = os.getenv('TELEGRAM_BOT_TOKEN')
my_request = Request(read_timeout=10)
bot = telegram.Bot(token = my_token, request = my_request)
updates = bot.getUpdates()
chat_id = os.getenv('CHAT_ID')

connection = {
    'host': os.getenv('CLICKHOUSE_HOST'),
    'password': os.getenv('CLICKHOUSE_PASSWORD'),
    'user': os.getenv('CLICKHOUSE_USER'),
    'database': os.getenv('CLICKHOUSE_DATABASE')
}

default_args = {
    'owner': 'e-chezhina',
    'retries': 2,
    'depends_on_past': False,
    'retry_delay': timedelta(minutes = 1),
    'start_date': datetime(2024, 12, 2)
}

schedule_interval = '0 11 * * 1'

@dag(default_args = default_args, schedule_interval = schedule_interval, catchup = False)
def dag_82_ck():
    
    @task
    def df_WAU_feed():
        q = """
        SELECT count(DISTINCT user_id) AS WAU,
               toMonday(time) AS week,
              source
        FROM {db}.feed_actions
        WHERE toMonday(time) BETWEEN toMonday(now() - INTERVAL 2 WEEK) AND toMonday(now() - INTERVAL 1 WEEK)
        GROUP BY week, source
        ORDER BY week"""

        df_WAU_feed = ph.read_clickhouse(q, connection = connection)
        return df_WAU_feed
    
    @task
    def df_WAU_messages():
        q = """
        SELECT count(DISTINCT user_id) AS WAU,
               toMonday(time) AS week,
              source
        FROM {db}.message_actions
        WHERE toMonday(time) BETWEEN toMonday(now() - INTERVAL 2 WEEK) AND toMonday(now() - INTERVAL 1 WEEK)
        GROUP BY week, source
        ORDER BY week"""

        df_WAU_messages = ph.read_clickhouse(q, connection = connection)
        return df_WAU_messages
    
    @task
    def df_lv_feed():
        q = ''' 
        SELECT 
         CASE WHEN toMonday(time) = toMonday(now() - INTERVAL 2 WEEK) THEN 'прошлая неделя'
              ELSE 'текущая_неделя' END AS week,
              COUNTIf(action = 'view') AS views,
              COUNTIf(action = 'like') AS likes,
              source
        FROM {db}.feed_actions
        WHERE toMonday(time) BETWEEN toMonday(now() - INTERVAL 2 WEEK) AND toMonday(now() - INTERVAL 1 WEEK)
        GROUP BY week, source
        ORDER BY week'''

        df_lv_feed = ph.read_clickhouse(q, connection = connection)
        return df_lv_feed
     
    @task
    def df_mi_message():
        q = ''' 
        SELECT 
         CASE WHEN toMonday(time) = toMonday(now() - INTERVAL 2 WEEK) THEN 'прошлая неделя'
              ELSE 'текущая_неделя' END AS week,
              COUNT(user_id) AS num_messages,
              source
        FROM {db}.message_actions
        WHERE toMonday(time) BETWEEN toMonday(now() - INTERVAL 2 WEEK) AND toMonday(now() - INTERVAL 1 WEEK)
        GROUP BY week, source
        ORDER BY week'''

        df_mi_message = ph.read_clickhouse(q, connection = connection)
        return df_mi_message
    
    @task
    def df_open_feed():
        q = """
        SELECT ROUND(COUNT(user_id)/ COUNT(DISTINCT user_id))::int AS actions_per_user,
               CASE WHEN toMonday(time) = toMonday(now() - INTERVAL 2 WEEK) THEN 'прошлая_неделя'
               ELSE 'текущая_неделя' END AS week,
               country
        FROM {db}.feed_actions
        WHERE toMonday(time) BETWEEN toMonday(now() - INTERVAL 2 WEEK) AND toMonday(now() - INTERVAL 1 WEEK)
        GROUP BY week, country
        ORDER BY country"""

        df_open_feed = ph.read_clickhouse(q, connection = connection)
        return df_open_feed
    
    @task
    def df_open_message():
        q = """
        SELECT ROUND(COUNT(user_id)/ COUNT(DISTINCT user_id))::int AS messages_per_user,
               CASE WHEN toMonday(time) = toMonday(now() - INTERVAL 2 WEEK) THEN 'прошлая_неделя'
               ELSE 'текущая_неделя' END AS week,
               country
        FROM {db}.message_actions
        WHERE toMonday(time) BETWEEN toMonday(now() - INTERVAL 2 WEEK) AND toMonday(now() - INTERVAL 1 WEEK)
        GROUP BY week, country
        ORDER BY country"""

        df_open_message = ph.read_clickhouse(q, connection = connection)
        return df_open_message
    
    @task
    def df_st():
        q = '''
        SELECT if(this_week = toMonday(now() - INTERVAL 1 WEEK), 'текущая_неделя', 'прошлая_неделя') AS week,
             this_week, previous_week, 
             COUNT(DISTINCT user_id) * -1 AS num_users, status, gender
        FROM
        (SELECT user_id, 
              groupUniqArray(toMonday(toDate(time))) AS weeks_visited, 
              arrayJoin(weeks_visited) AS previous_week,
              addWeeks(previous_week, +1) AS this_week,
              'ушедшие' AS status,
              if(gender = 1, 'женщина', 'мужчина') AS gender
        FROM simulator_20241020.feed_actions
        WHERE toMonday(time) BETWEEN toMonday(now() - INTERVAL 3 WEEK) AND toMonday(now() - INTERVAL 1 WEEK)
        GROUP BY user_id, gender
        HAVING NOT has(weeks_visited, this_week))
        GROUP BY this_week, previous_week, status, gender
        HAVING this_week = toMonday(now() - INTERVAL 1 WEEK) or this_week = toMonday(now() - INTERVAL 2 WEEK)

        UNION ALL

        SELECT if(this_week = toMonday(now() - INTERVAL 1 WEEK), 'текущая_неделя', 'прошлая_неделя') AS week,
               this_week, previous_week,
               toInt64(COUNT(DISTINCT user_id)) AS num_users, status, gender
        FROM
        (SELECT user_id,
              groupUniqArray(toMonday(toDate(time))) AS weeks_visited,
              arrayJoin(weeks_visited) AS this_week,
              addWeeks(this_week, -1) AS previous_week,
              if(has(weeks_visited, previous_week) = 1, 'оставшиеся', 'новые') AS status,
              if(gender = 1, 'женщина', 'мужчина') AS gender
        FROM simulator_20241020.feed_actions
        WHERE toMonday(time) BETWEEN toMonday(now() - INTERVAL 3 WEEK) AND toMonday(now() - INTERVAL 1 WEEK)
        GROUP BY user_id, gender)
        GROUP BY this_week, previous_week, week, status, gender
        HAVING this_week = toMonday(now() - INTERVAL 1 WEEK) or this_week = toMonday(now() - INTERVAL 2 WEEK)
        '''

        df_st = ph.read_clickhouse(q, connection = connection)
        df_st['week_gender'] = df_st['week'] + ' - ' + df_st['gender']
        df_st = df_st.groupby(['status', 'week_gender'], as_index = False).agg({'num_users': 'mean'})
        df_st['week_gender'] = pd.Categorical(df_st['week_gender'], 
                                              categories=['прошлая_неделя - женщина', 'текущая_неделя - женщина', 
                                                          'прошлая_неделя - мужчина', 'текущая_неделя - мужчина'], 
                                              ordered=True)
        df_st = df_st.pivot_table(index='week_gender', columns='status', values='num_users', fill_value=0).reset_index()
        return df_st
    
    @task
    def df_st_m():
        q = '''
        SELECT if(this_week = toMonday(now() - INTERVAL 1 WEEK), 'текущая_неделя', 'прошлая_неделя') AS week,
             this_week, previous_week, 
             COUNT(DISTINCT user_id) * -1 AS num_users, status, gender
        FROM
        (SELECT user_id, 
              groupUniqArray(toMonday(toDate(time))) AS weeks_visited, 
              arrayJoin(weeks_visited) AS previous_week,
              addWeeks(previous_week, +1) AS this_week,
              'ушедшие' AS status,
              if(gender = 1, 'женщина', 'мужчина') AS gender
        FROM {db}.message_actions
        WHERE toMonday(time) BETWEEN toMonday(now() - INTERVAL 3 WEEK) AND toMonday(now() - INTERVAL 1 WEEK)
        GROUP BY user_id, gender
        HAVING NOT has(weeks_visited, this_week))
        GROUP BY this_week, previous_week, status, gender
        HAVING this_week = toMonday(now() - INTERVAL 1 WEEK) or this_week = toMonday(now() - INTERVAL 2 WEEK)

        UNION ALL

        SELECT if(this_week = toMonday(now() - INTERVAL 1 WEEK), 'текущая_неделя', 'прошлая_неделя') AS week,
               this_week, previous_week,
               toInt64(COUNT(DISTINCT user_id)) AS num_users, status, gender
        FROM
        (SELECT user_id,
              groupUniqArray(toMonday(toDate(time))) AS weeks_visited,
              arrayJoin(weeks_visited) AS this_week,
              addWeeks(this_week, -1) AS previous_week,
              if(has(weeks_visited, previous_week) = 1, 'оставшиеся', 'новые') AS status,
              if(gender = 1, 'женщина', 'мужчина') AS gender
        FROM {db}.message_actions
        WHERE toMonday(time) BETWEEN toMonday(now() - INTERVAL 3 WEEK) AND toMonday(now() - INTERVAL 1 WEEK)
        GROUP BY user_id, gender)
        GROUP BY this_week, previous_week, week, status, gender
        HAVING this_week = toMonday(now() - INTERVAL 1 WEEK) or this_week = toMonday(now() - INTERVAL 2 WEEK)
        '''

        df_st_m = ph.read_clickhouse(q, connection = connection)
        df_st_m['week_gender'] = df_st_m['week'] + ' - ' + df_st_m['gender']
        df_st_m = df_st_m.groupby(['status', 'week_gender'], as_index = False).agg({'num_users': 'mean'})
        df_st_m['week_gender'] = pd.Categorical(df_st_m['week_gender'], 
                                              categories=['прошлая_неделя - женщина', 'текущая_неделя - женщина', 
                                                          'прошлая_неделя - мужчина', 'текущая_неделя - мужчина'], 
                                              ordered=True)
        df_st_m = df_st_m.pivot_table(index='week_gender', columns='status', values='num_users', fill_value=0).reset_index()
        return df_st_m
    
    @task
    def graph_WAU(df_WAU_feed, df_WAU_messages):
        plt.figure(figsize=(12, 10))

        plt.subplot(2, 1, 1)
        sns.lineplot(data = df_WAU_feed, x = 'week', y = 'WAU', hue = 'source', palette='cubehelix', style="source",
            markers= ['o', 'o'], dashes = False)
        plt.title('WAU ленты новостей по источнику трафика за последние 2 недели', fontweight='bold', fontsize=12, pad=10)
        plt.xlabel('')
        plt.xticks(ticks=[df_WAU_feed['week'].iloc[0], df_WAU_feed['week'].iloc[-1]])
        plt.ylabel('')
        plt.legend(title = '', loc='upper left')
        plt.tight_layout()

        plt.subplot(2, 1, 2)
        sns.lineplot(data = df_WAU_messages, x = 'week', y = 'WAU', hue = 'source', palette='cubehelix', style="source",
            markers= ['o', 'o'], dashes = False)
        plt.title('WAU ленты сообщений по источнику трафика за последние 2 недели', fontweight='bold', fontsize=12, pad=10)
        plt.xlabel('')
        plt.xticks(ticks=[df_WAU_messages['week'].iloc[0], df_WAU_messages['week'].iloc[-1]])
        plt.ylabel('')
        plt.legend(title = '', loc='upper left')
        plt.tight_layout()
        
        plot_object_WAU = io.BytesIO()
        plt.savefig(plot_object_WAU)
        plot_object_WAU.seek(0)
        plot_object_WAU.name = "WAU.png"
        plt.close()
        return plot_object_WAU
    
    @task
    def graph_lvmi(df_lv_feed, df_mi_message):
        plt.figure(figsize=(12, 16))

        plt.subplot(4, 1, 1)
        sns.barplot(data = df_lv_feed, x = 'week', y = 'views', hue = 'source', palette='summer')
        plt.title('Количество просмотров по источнику трафика (прошлая vs текущая недели)', fontweight='bold', fontsize=12, pad=10)
        plt.xlabel('')
        plt.ylabel('')
        plt.legend(title = '', loc='upper left')
        # Форматирование оси Y для отображения чисел
        def millions(x, pos):
            return '%1.0f' % (x)  # Отображение миллиона без "le6"

        formatter = FuncFormatter(millions)
        plt.gca().yaxis.set_major_formatter(formatter)

        plt.subplot(4, 1, 2)
        sns.barplot(data = df_lv_feed, x = 'week', y = 'likes', hue = 'source', palette='copper')
        plt.title('Количество лайков по источнику трафика (прошлая vs текущая недели)', fontweight='bold', fontsize=12, pad=10)
        plt.xlabel('')
        plt.ylabel('')
        plt.legend(title = '', loc='upper left')


        plt.subplot(4, 1, 3)
        sns.barplot(data = df_mi_message, x = 'week', y = 'num_messages', hue = 'source', palette='gist_earth')
        plt.title('Количество отправленных сообщений по источнику трафика (прошлая vs текущая недели)', fontweight='bold', fontsize=12, pad=10)
        plt.xlabel('')
        plt.ylabel('')
        plt.legend(title = '', loc='upper left')

        plt.tight_layout()
        
        plot_object_lvmi = io.BytesIO()
        plt.savefig(plot_object_lvmi)
        plot_object_lvmi.seek(0)
        plot_object_lvmi.name = "Views_likes_messages.png"
        plt.close()
        return plot_object_lvmi
    
    @task
    def graph_country(df_open_feed, df_open_message):
        plt.figure(figsize=(16, 10))

        plt.subplot(1,2,1)
        sns.barplot(data = df_open_feed, x = 'actions_per_user', y = 'country', hue = 'week', palette='copper')
        plt.title('Количество взаимодействий с лентой новостей на пользователя по странам', fontweight='bold', fontsize=12, pad=10)
        plt.xlabel('')
        plt.ylabel('')
        plt.legend(title = '', loc='upper right')

        plt.subplot(1,2,2)
        sns.barplot(data = df_open_message, x = 'messages_per_user', y = 'country', hue = 'week', palette='gist_earth')
        plt.title('Количество сообщений на пользователя по странам', fontweight='bold', fontsize=12, pad=10)
        plt.xlabel('')
        plt.ylabel('')
        plt.legend(title = '', loc='upper right')
        
        plot_object_country = io.BytesIO()
        plt.savefig(plot_object_country)
        plot_object_country.seek(0)
        plot_object_country.name = "Opens_by_countries.png"
        plt.close()
        return plot_object_country
    
    @task
    def graph_st(df_st, df_st_m):
        
        plt.figure(figsize=(12, 12))

        plt.subplot(2, 1, 1)
        colors = ['#8c8b88', '#e8b15f', '#6da372']
        ax = df_st.plot(kind='bar', stacked=True, ax=plt.gca(), color=colors)  # Использовать текущую ось

        plt.title('Лента новостей: динамика поведения пользователей по статусу и полу', fontweight='bold', fontsize=12, pad=10)
        plt.legend(title='Статус')
        plt.axhline(0, color='black', linewidth=0.8, linestyle='--')
        new_labels = ['Прошлая неделя(женщины)', 'Текущая неделя(женщины)', 'Прошлая неделя(мужчины)', 'Текущая неделя(мужчины)']
        ax.set_xticks(range(len(new_labels)))
        ax.set_xticklabels(new_labels)
        plt.xticks(rotation=360)


        plt.subplot(2, 1, 2)
        ax_1 = df_st_m.plot(kind='bar', stacked=True, ax=plt.gca(), color=colors)  # Использовать текущую ось

        plt.title('Лента сообщений: динамика поведения пользователей по статусу и полу', fontweight='bold', fontsize=12, pad=10)
        plt.legend(title='Статус')
        plt.axhline(0, color='black', linewidth=0.8, linestyle='--')
        new_labels = ['Прошлая неделя(женщины)', 'Текущая неделя(женщины)', 'Прошлая неделя(мужчины)', 'Текущая неделя(мужчины)']
        ax_1.set_xticks(range(len(new_labels)))
        ax_1.set_xticklabels(new_labels)
        plt.xticks(rotation=360)

        plt.tight_layout()
        
        plot_object_st = io.BytesIO()
        plt.savefig(plot_object_st)
        plot_object_st.seek(0)
        plot_object_st.name = "Status_gender_statistic.png"
        plt.close()
        return plot_object_st
    
    @task
    def send_telegram(plot_object_WAU, plot_object_lvmi, plot_object_country, plot_object_st):
        bot.send_photo(chat_id = chat_id, photo = plot_object_WAU)
        bot.send_photo(chat_id = chat_id, photo = plot_object_lvmi)
        bot.send_photo(chat_id = chat_id, photo = plot_object_country)
        bot.send_photo(chat_id = chat_id, photo = plot_object_st)
    
    df_WAU_feed = df_WAU_feed()
    df_WAU_messages = df_WAU_messages()
    df_lv_feed = df_lv_feed()
    df_mi_message = df_mi_message()
    df_open_feed = df_open_feed()
    df_open_message = df_open_message()
    df_st = df_st()
    df_st_m = df_st_m()
    
    plot_object_WAU = graph_WAU(df_WAU_feed, df_WAU_messages)
    plot_object_lvmi = graph_lvmi(df_lv_feed, df_mi_message)
    plot_object_country = graph_country(df_open_feed, df_open_message)
    plot_object_st = graph_st(df_st, df_st_m)
    send_telegram(plot_object_WAU, plot_object_lvmi, plot_object_country, plot_object_st)

dag_82_ck = dag_82_ck()
        
        
        
