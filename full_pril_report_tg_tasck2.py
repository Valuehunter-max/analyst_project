# Подключаемся к БД, соеднияем информацию с двух таблиц 
# Считаем ключевые метрики со всего прилажения за вчерашний день
# Строим график за последние 7 дней.
# Отправляем отчет с информацией и графиками в телеграм по расписанию через gitlab



import pandas as pd
import pandahouse
import seaborn as sns
import matplotlib.pyplot as plt
import telegram
import io
import os


sns.set(
    font_scale=1,
    style="whitegrid",
    rc={'figure.figsize':(12,8)}
        )
dir_path = '/home/jupyter-v.akinchits-10/analyst_simulator' 

def report_metriki_full(chat=None):
    chat_id = chat or ******
    connection = {
            'host': '********',
            'password': '*******',
            'user': '*******',
            'database': '******'
            }

    q1 = '''
    SELECT 
        toStartOfDay(time) AS date,
        count(DISTINCT user_id) as dau,
        COUNTIf(DISTINCT user_id, category='new') AS new_users,
        count(action) as fuul_actions,
        -- countIf(action='like') AS likes,
        -- countIf(action='view') AS views,
        -- countIf(action='mess') AS messsges,
        floor(count(action) / count(DISTINCT user_id), 2) AS mean_user_actions,
        floor(countIf(action='view')/ count(DISTINCT user_id), 2) AS mean_user_views,
        floor(countIf(action='like')/ count(DISTINCT user_id), 2) AS mean_user_likes,
        floor(countIf(action='mess')/ count(DISTINCT user_id), 2) AS mean_user_messages

    FROM 
        (SELECT 
          user_id,
          time,
          action,
          (time - date_reg) AS distance,
          CASE 
            WHEN distance = 0 THEN 'new'
            ELSE 'old'
            END AS category
        FROM 
            (SELECT
              user_id,
              time,
              action
            FROM simulator.feed_actions 

            UNION ALL 
            SELECT 
              user_id,
              time,
              CAST('mess' AS String) AS action
            FROM 
              simulator.message_actions) AS A
        LEFT JOIN    
        (SELECT 
          user_id,
          MIN(time) AS date_reg
        FROM
          (SELECT
              user_id,
              time,
              action
            FROM simulator.feed_actions 

            UNION ALL 
            SELECT 
              user_id,
              time,
              CAST('mess' AS String) AS action
            FROM 
              simulator.message_actions)
        GROUP BY 
          user_id) AS B
        ON A.user_id = B.user_id )
    GROUP BY 
    date
    HAVING date = yesterday()
    '''
    data_full = pandahouse.read_clickhouse(q1, connection=connection)
    
    #бот для отравки сообщений
    bot = telegram.Bot(token='*************')
    mess = '''
    Привет) Отчет по ключемы показателям по всему приложению за вчера {}:
    Дневная аудитория: {}
    Новых пользователей: {}
    Всего событий: {}
    Среднее число событий на 1-го пользователя: {}
    Среднее число просмотров на 1-го пользователя: {}
    Среднее число лайков на 1-го пользователя: {}
    Среднее число сообщений на 1-го пользователя: {}
    '''.format(data_full['date'][0],
               data_full['dau'][0],
               data_full['new_users'][0],
               data_full['fuul_actions'][0],
               data_full['mean_user_actions'][0],
               data_full['mean_user_views'][0],
               data_full['mean_user_likes'][0],
               data_full['mean_user_messages'][0])
    bot.sendMessage(chat_id=chat_id, text=mess)

    #Графики за последние 7 дней и бот отправления график в телеграм.
    q2 = '''
    SELECT 
        toStartOfDay(time) AS date,
        count(DISTINCT user_id) as dau,
        COUNTIf(DISTINCT user_id, category='new') AS new_users,
        count(action) as fuul_actions,
        -- countIf(action='like') AS likes,
        -- countIf(action='view') AS views,
        -- countIf(action='mess') AS messsges,
        floor(count(action) / count(DISTINCT user_id), 2) AS mean_user_actions,
        floor(countIf(action='view')/ count(DISTINCT user_id), 2) AS mean_user_views,
        floor(countIf(action='like')/ count(DISTINCT user_id), 2) AS mean_user_likes,
        floor(countIf(action='mess')/ count(DISTINCT user_id), 2) AS mean_user_messages

    FROM 
        (SELECT 
          user_id,
          time,
          action,
          (time - date_reg) AS distance,
          CASE 
            WHEN distance = 0 THEN 'new'
            ELSE 'old'
            END AS category
        FROM 
            (SELECT
              user_id,
              time,
              action
            FROM **

            UNION ALL 
            SELECT 
              user_id,
              time,
              CAST('mess' AS String) AS action
            FROM 
              simulator.message_actions) AS A
        LEFT JOIN    
        (SELECT 
          user_id,
          MIN(time) AS date_reg
        FROM
          (SELECT
              user_id,
              time,
              action
            FROM ***

            UNION ALL 
            SELECT 
              user_id,
              time,
              CAST('mess' AS String) AS action
            FROM 
              simulator.message_actions)
        GROUP BY 
          user_id) AS B
        ON A.user_id = B.user_id )
    GROUP BY 
    date
    HAVING date >= (yesterday()-6) AND date < today()
    '''
    data_full_yesterday = pandahouse.read_clickhouse(q2, connection=connection)


    #график дневной аудитории
    sns.lineplot(data=data_full_yesterday, x='date', y='dau')
    plt.title('Кол-во уникальных пользователей по дням')
    plt_dau_full = io.BytesIO()
    plt.savefig(plt_dau_full)
    plt_dau_full.name = 'plt_dau_full.png'
    plt_dau_full.seek(0)
    plt.close()
    bot.sendPhoto(chat_id=chat_id, photo=plt_dau_full)

    #график новых пользователей
    sns.lineplot(data=data_full_yesterday, x='date', y='new_users')
    plt.title('Кол-во новых пользователей')
    plt_new_users_full = io.BytesIO()
    plt.savefig(plt_new_users_full)
    plt_dau_full.name = 'plt_new_users_full.png'
    plt_new_users_full.seek(0)
    plt.close()
    bot.sendPhoto(chat_id=chat_id, photo=plt_new_users_full)

    #график событий 
    sns.lineplot(data=data_full_yesterday, x='date', y='fuul_actions')
    plt.title('Кол-во всех событий по дням')
    plt_fuul_actions = io.BytesIO()
    plt.savefig(plt_fuul_actions)
    plt_fuul_actions.name = 'plt_fuul_actions.png'
    plt_fuul_actions.seek(0)
    plt.close()
    bot.sendPhoto(chat_id=chat_id, photo=plt_fuul_actions)

    #Среднее число событий на 1-го пользователя.
    sns.lineplot(data=data_full_yesterday, x='date', y='mean_user_actions')
    plt.title('Среднее кол-во событий на 1-го пользователя')
    plt_mean_user_actions = io.BytesIO()
    plt.savefig(plt_mean_user_actions)
    plt_mean_user_actions.name = 'plt_mean_user_actions.png'
    plt_mean_user_actions.seek(0)
    plt.close()
    bot.sendPhoto(chat_id=chat_id, photo=plt_mean_user_actions)

    #Cреднее число каждого события
    sns.lineplot(data=data_full_yesterday, x='date', y='mean_user_views')
    sns.lineplot(data=data_full_yesterday, x='date', y='mean_user_likes')
    sns.lineplot(data=data_full_yesterday, x='date', y='mean_user_messages')
    plt.title('Среднее кол-во событий в разрезе каждого события')
    plt.legend(['mean_user_views', 'mean_user_likes', 'mean_user_messages' ])
    plt_mean_user_action_agg = io.BytesIO()
    plt.savefig(plt_mean_user_action_agg)
    plt_mean_user_action_agg.name = 'plt_mean_user_action_agg.png'
    plt_mean_user_action_agg.seek(0)
    plt.close(0)
    bot.sendPhoto(chat_id=chat_id, photo=plt_mean_user_action_agg)
    return
    


try:
    report_metriki_full()
except Exception as e:
    print(e)
