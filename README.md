### Общее описание задачи
Требуется написать DAG, который будет считать еженедельные метрики по ленте новостей (feed_actions) и ленте сообщений (message_actions) в сравнении с предыдущей неделей. Отчет (в виде графиков) должен приходить в чат в телеграм по понедельникам в 11:00.

### Описание данных
Лента новостей (feed_actions):
 - `user_id` - идентификатор пользователя
 - `post_id` - идентификатор поста ленты новостей
 - `action` - действие пользователя (view - просмотр, like - лайк)
 - `time` - дата и время взаимодействия с постом ленты новостей
 - `gender` - пол (1 - женщина, 0 - мужчина)
 - `age` - возраст
 - `country` - страна
 - `city` - город
 - `os` - операционная система (Android или iOS)
 - `source` - источник трафика (organic - обычная поисковая выдача, ads - рекламное объявление)
 - `exp_group` - группа для эксперимента (0, 1, 2, 3 или 4)

Лента сообщений (message_actions):
 - `user_id` - идентификатор пользователя
 - `reciever_id` - индентификатор пользователя, который получил сообщение
 - `time` - дата и время отправки сообщения
 - `gender` - пол (1 - женщина, 0 - мужчина)
 - `age` - возраст
 - `country` - страна
 - `city` - город
 - `os` - операционная система (Android или iOS)
 - `source` - источник трафика (organic - обычная поисковая выдача, ads - рекламное объявление)
 - `exp_group` - группа для эксперимента (0, 1, 2, 3 или 4)

### Этапы реализации задачи ([код здесь]())

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

**Пример датасетов после выполнения тасок**:

![st_f](https://github.com/Kateri-Che/weekly-reports-telegram/blob/main/gender_status_feed.png) ![st_m](https://github.com/Kateri-Che/weekly-reports-telegram/blob/main/gender_status_messages.png)

**2.** Создание графиков.

**Реализовано в тасках `graph_WAU `, `graph_lvmi`, `graph_country`, `graph_st`**.

**3.** Выгрузка графиков в телеграм.

**Реализовано в таске `send_telegram`**.
