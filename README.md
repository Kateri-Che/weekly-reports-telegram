### Контекст
Компании требуется автоматизировать отчетность своих еженедельных метрик, отчет по ним должен приходить в чат в телеграм по понедельникам в 11:00.

### Задача проекта
Написать DAG, который будет считать еженедельные метрики по новостной ленте (feed_actions) и ленте сообщений (message_actions) в сравнении с прошлой неделей и отправлять их в чат-телеграм.

Текущая неделя - неделя, которая началась в прошлый понедельник и только что завершилась, прошлая неделя - это 2 недели назад.

### Cтек
 - airflow
 - telegram
 - pandas
 - pandahouse
 - clickhouse
 - io
 - seaborn
 - matplotlib
 - os

### Этапы реализации проекта ([посмотреть код](https://github.com/Kateri-Che/weekly-reports-telegram/blob/main/dag_telegram_report.py))

**1. Подсчет метрик и формирование графиков**:

**1.1.** В разрезе **по источнику трафика (обычная поисковая выдача, рекламное объявление)** :
- WAU ленты новостей.
- WAU ленты сообщений.

**Реализовано в тасках `df_WAU_feed`, `df_WAU_messages`**.

**Пример датасетов после выполнения тасок (лента новостей, лента сообщений)**:

![WAU_f](https://github.com/Kateri-Che/weekly-reports-telegram/blob/main/WAU_feed.png)    ![WAU_m](https://github.com/Kateri-Che/weekly-reports-telegram/blob/main/WAU_messages.png)

-	Количество просмотров.
-	Количество лайков.
-	Количество сообщений.

**Реализовано в тасках `df_lv_feed`, `df_mi_message`**.

**Пример датасетов после выполнения тасок**:

![lv_f](https://github.com/Kateri-Che/weekly-reports-telegram/blob/main/likes_views.png) ![m](https://github.com/Kateri-Che/weekly-reports-telegram/blob/main/messages.png)

**1.2.** В разрезе **по странам**:
- Количество взаимодействий с лентой новостей на пользователя. 
- Количество взаимодействий с лентой сообщений на пользователя.
  
**Реализовано в тасках `df_open_feed`, `df_open_message`**.
  
**Пример датасетов после выполнения тасок**:

![a_f](https://github.com/Kateri-Che/weekly-reports-telegram/blob/main/actions_feed.png) ![a_m](https://github.com/Kateri-Che/weekly-reports-telegram/blob/main/actions_messages.png)

**1.3.** В разрезе **по полу** и **статусу (новые, оставшиеся, ушедшие)**:
- Динамика поведения пользователей ленты новостей.
- Динамика поведения пользователей ленты сообщений.  

**Реализовано в тасках `df_st`, `df_st_m`**.

**Пример датасетов после выполнения тасок(лента новостей, лента сообщений)**:

![st_f](https://github.com/Kateri-Che/weekly-reports-telegram/blob/main/gender_status_feed.png) ![st_m](https://github.com/Kateri-Che/weekly-reports-telegram/blob/main/gender_status_messages.png)

**2.** Создание графиков.

**Реализовано в тасках `graph_WAU `, `graph_lvmi`, `graph_country`, `graph_st`**.

**3.** Выгрузка графиков в телеграм.

**Реализовано в таске `send_telegram`**.

### Результат
Результатом выполнения проекта стало создание DAG-а, благодаря которому:
 - сокращено время, затрачиваемое на ручное составление отчетов;
 - реализовано регулярное отслеживание текущих показателей ключевых метрик бизнеса;
 - своевременно выявлявляются изменения в метриках от недели к неделе для корректного выстраивания дальнейшей стратегии бизнеса.

**Граф в Airflow**:

![dag:](https://github.com/Kateri-Che/weekly-reports-telegram/blob/main/telegram_report_dag.png)

**Графики, отправленные в чат в Telegram**:

![viz_1:](https://github.com/Kateri-Che/weekly-reports-telegram/blob/main/screen_1.jpg)  ![viz_2:](https://github.com/Kateri-Che/weekly-reports-telegram/blob/main/screen_2.jpg)

**Рассмотреть графики поближе: [открыть ссылку](https://drive.google.com/drive/folders/1a_bX1HS6_3_ffbbwRxv3E6YKDc1XEJBD?usp=sharing)**
