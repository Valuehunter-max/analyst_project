# Данный скрипт позволяет оперативно реагировать на аномально низкие и высокие значения отслеживаемых метрик

# 1) Делаем запрос к БД, в котором получаем значение метрик за предпоследние 15 минут.
# Т.е. если сейчас 15:31, тогда мы будем получать данные за последние 2 недели в диапазоне
# времени 15:15 - 15:30
# 2) Строим скользящее среднее по каждой из метрик.
# 3) Считаем доверительные интервалы.
# 4) Если текущее значение метрики выходит за границы интервала, посылаем сообщение
# в телеграм о то, что есть аномалия и необходимо отрегировать.
# 5) Запускаем крон, который будет каждые 15 минут будет выполнять скрит


import pandas as pd
import pandahouse
import telegram
import numpy as np


connection = {
            'host': '*******,
            'password': '*********',
            'user': '********',
            'database': '*******'
            }

q1 = '''
SELECT 
A.15_min AS time,
B.activ_user AS activ_users,
B.actions AS actions,
B.view AS views,
B.likes AS likes,
B.CTR AS ctr
FROM 
  (SELECT 
  DISTINCT toStartOfDay(time) AS day_time,
  toStartOfFifteenMinutes(time) AS 15_min,
  toStartOfFifteenMinutes(time) -  toDateTime(toStartOfDay(time)) AS time_div
  FROM 
  simulator.feed_actions 
  WHERE time_div = 
  
    (SELECT 
    time_div
    FROM
      (SELECT 
          DISTINCT toStartOfDay(time) AS day_time,
          toStartOfFifteenMinutes(time) AS 15_min,
          toStartOfFifteenMinutes(time) -  toDateTime(toStartOfDay(time)) AS time_div
      FROM 
          simulator.feed_actions 
      WHERE 15_min = (SELECT Max(toStartOfFifteenMinutes(time) - 900) FROM  simulator.feed_actions)
  ))
  ) AS A 

LEFT JOIN 
(SELECT 
  toStartOfFifteenMinutes(time) AS 15_min,
  count(DISTINCT user_id) AS activ_user,
  count(action) AS actions,
  countIf(user_id, action ='view') AS view,
  countIf(user_id, action = 'like') AS likes,
  countIf(user_id, action = 'like') / countIf(user_id, action = 'view') AS CTR
FROM 
    simulator.feed_actions 
    GROUP BY 15_min) AS B

ON A.15_min = B.15_min
WHERE toDate(day_time) >= today() - 13
ORDER BY 15_min ASC

'''

df = pandahouse.read_clickhouse(q1, connection=connection)
bot = telegram.Bot(token='**************')
chat_id = ************

#Для удобства операций с каждой метрикой, сохраним информацию о метрике и времени в отдельные таблицы
df_activ_users = df[['time', 'activ_users']]
df_views = df[['time', 'views']]
df_likes = df[['time', 'likes']]
df_ctr = df[['time', 'ctr']]

#Сохраним текущий таймлайн в отдельную переменную, для того, что бы в дальнейшем создавать отчет.
max_time = df.time.max()

def detect_anomaly(data, metric, metric_name_rus):
    data_anomaly = data
    data_anomaly['rolling_mean'] = data_anomaly[metric].rolling(window=3).mean()
    data_anomaly['differene'] = data_anomaly['rolling_mean'] - data_anomaly[metric]
    difference_std = np.std(data_anomaly['differene'])
    data_anomaly['upper_bound'] = data_anomaly['rolling_mean'] + difference_std * 2.576
    data_anomaly['lower_bound'] = data_anomaly['rolling_mean'] - difference_std * 2.576
    
    #Оставим строку с датой, по которой будем отслеживать аномальное значение.
    data_anomaly_totime = data_anomaly.loc[data_anomaly['time'] == max_time].reset_index(drop=True)
    
    #Проверяем является ли значение метрики аномальной, если да, отправляем информацию в телеграм
    if data_anomaly_totime[metric][0] > data_anomaly_totime['upper_bound'][0]:
        anomaly_differnce = (data_anomaly_totime[metric][0] - data_anomaly[metric].mean()) / data_anomaly[metric].mean()
        mess_tg = '''
НАЙДЕНО АНОМАЛЬНОЕ ЗНАЧЕНИЕ МЕТРИКИ!!!!!!

Метрика: {} от {}
выше среднего за последние 10 дней на: {:.2%}
текущее значение метрики: {:.2f}
среднее значение за последние 14 дней: {:.2f}
ссылка на дашборд: https://superset.lab.karpov.courses/superset/dashboard/15/
'''.format(metric_name_rus,
           max_time,
           anomaly_differnce,
           data_anomaly_totime[metric][0],
           data_anomaly[metric].mean()
             )
        bot.sendMessage(chat_id=chat_id, text=mess_tg)
    elif data_anomaly_totime[metric][0] < data_anomaly_totime['lower_bound'][0]:
        anomaly_differnce = (data_anomaly[metric].mean() - data_anomaly_totime[metric][0]) / data_anomaly_totime[metric][0]
        mess_tg = '''
НАЙДЕНО АНОМАЛЬНОЕ ЗНАЧЕНИЕ МЕТРИКИ!!!!!!

Метрика: {} от {}
ниже  среднего за последние 14 дней на: {:.2%}
текущее значение метрики: {:.2f}
среднее значение за последние 10 дней: {:.2f}
ссылка на дашборд: https://superset.lab.karpov.courses/superset/dashboard/15/
'''.format(metric_name_rus,
           max_time,
           anomaly_differnce,
           data_anomaly_totime[metric][0],
           data_anomaly[metric].mean()
             )
        bot.sendMessage(chat_id=chat_id, text=mess_tg)
    
    else:
        bot.sendMessage(chat_id=chat_id, text='в метрике: {} аномалий нет'.format(metric))
          
    return 

detect_anomaly(df_activ_users, 'activ_users', 'кол-во активных пользователей')
detect_anomaly(df_views, 'views', 'кол-во просмотров постов')
detect_anomaly(df_likes, 'likes', 'кол-во лайков' )
detect_anomaly(df_ctr, 'ctr', 'ctr' )
