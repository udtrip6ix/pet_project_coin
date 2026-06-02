1. Запуск проекта
Сначала соберите контейнеры и запустите сервисы:

# Сборка образов
docker-compose build

# Запуск в фоновом режиме
docker-compose up -d

2. Создание пользователя Airflow
После запуска контейнеров необходимо создать администратора Airflow, чтобы получить доступ к панели управления:

docker exec -it pet_project_coin-airflow-webserver-1 airflow users create \
    --username airflow \
    --firstname Airflow \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password airflow


3. Настройка Airflow (Admin Panel)
После того как вы залогинитесь в Airflow (http://localhost:8080), необходимо настроить переменные и подключения.

    3.1. Variables (Admin -> Variables)
    Создайте следующие переменные:


    access_key  : [ВАШ_ACCESS_KEY_ИЗ_MINIO]
    secret_key  : [ВАШ_SECRET_KEY_ИЗ_MINIO]
    ch_user     : admin
    ch_password : [ВАШ_ПАРОЛЬ_CLICKHOUSE]



     Как получить ключи для S3 (MinIO):
    Перейдите в консоль MinIO (обычно http://localhost:9001), откройте раздел Access Keys, нажмите Create access key и скопируйте полученные значения в соответствующие переменные Airflow.

    3.2. Connections (Admin -> Connections)


    ClickHouse (clickhouse_default)
    {
    "Connection Id": "clickhouse_default",
    "Connection Type": "Generic",
    "Host": "clickhouse",
    "Schema": "pet_analytics",
    "Login": "admin",
    "Password": "[ВАШ_ПАРОЛЬ_CLICKHOUSE]",
    "Port": 8123
    }

    Spark (spark_default)
    {
    "Connection Id": "spark_default",
    "Connection Type": "Spark",
    "Host": "spark://spark-master",
    "Port": 7077,
    "Extra": {
        "spark-submit-options": "--master spark://spark-master:7077 --deploy-mode client"
        }
    }

4. Настройка Metabase
Для визуализации данных перейдите в Metabase (http://localhost:3000).