#Скрипт для отправки отчета по ключевым метрикам приложения.
#А) Считаем показатели за вчерашний день.
#Б) Строим графики за последние 7 дней
#В) Отправляем отчет каждый день в 11:00. Cron запускается с gitlab



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
    rc={'figure.figsize':(12,8)})



def report_metriki_feed_action(chat=None):
    chat_id = chat or **********
    connection = {
        'host': '********',
        'password': '*******',
        'user': '*********',
        'database': '***********'
        }

    q1 = '''
    SELECT 
      toStartOfDay(time) AS date,
      COUNT(DISTINCT user_id) AS dau,
      COUNTIf(action='view') AS view,
      COUNTIf(action='like') AS like,
      floor(COUNTIf(user_id, action='like') / COUNTIf(user_id, action='view')*100, 2) AS ctr

    FROM 
      **********
    GROUP BY 
      date
    HAVING 
      date = yesterday()
      '''
    dau_yester_day = pandahouse.read_clickhouse(q1, connection=connection)

    bot = telegram.Bot(token='**************')
    msg = ' Лента новостей. Показатели за вчерашний день:\ndau - {}\nview - {}\nlike - {}\nctr - {}%'.format(dau_yester_day['dau'][0],
                                                                 dau_yester_day['view'][0],
                                                                 dau_yester_day['like'][0],
                                                                 dau_yester_day['ctr'][0])
    bot.sendMessage(chat_id=chat_id, text=msg)


    #Бот по отправке графиков
    q2 = '''
    SELECT 
        toStartOfDay(time) AS date,
      COUNT(DISTINCT user_id) AS dau,
      COUNTIf(action='view') AS view,
      COUNTIf(action='like') AS like,
      floor(COUNTIf(user_id, action='like') / COUNTIf(user_id, action='view')*100, 2) AS ctr
    FROM 
      *******
    GROUP BY 
      date
    HAVING 
      date >= (yesterday()-6) and date < today()
      '''
    dau_7_day = pandahouse.read_clickhouse(q2, connection=connection)
    
    sns.lineplot(data=dau_7_day, x='date', y='dau')
    plt.title("дневная аудитория за последние 7 дней включая вчера")
    plt_dau = io.BytesIO()  #создаем объект в оперативной памяти.
    plt.savefig(plt_dau)  #сохраняем график в оперативную память.
    plt_dau.name = 'plot_dau.png'  #даем название файлу
    plt_dau.seek(0)  #перемещаем курсор в начало объекта
    plt.close() #закрываем график (вроде)
    bot.sendPhoto(chat_id=chat_id, photo=plt_dau)

    sns.lineplot(data=dau_7_day, x='date', y='view')
    plt.title("Кол-во показов постов за последние 7 дней включая вчера")
    plt_view = io.BytesIO() #создаем объект
    plt.savefig(plt_view)  #сохраняем график в оп. память
    plt_view.name = 'plt_view.png' #пишем название файла
    plt_view.seek(0)
    plt.close()
    bot.sendPhoto(chat_id=chat_id, photo=plt_view)

    sns.lineplot(data=dau_7_day, x='date', y='like')
    plt.title("Кол-во лайков постов за последние 7 дней включая вчера")
    plt_like = io.BytesIO()
    plt.savefig(plt_like)
    plt_like.name = 'plot_like'
    plt_like.seek(0)
    plt.close()
    bot.sendPhoto(chat_id=chat_id, photo=plt_like)
    return
    
try:
    report_metriki_feed_action()
except Exception as e:
    print(e)
