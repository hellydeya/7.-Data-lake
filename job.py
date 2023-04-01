import pyspark
import sys
import pyspark.sql.functions as F
import subprocess
import pytz
import logging
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import countDistinct
from pyspark.sql.types import DateType
from datetime import datetime, timedelta
from math import radians, asin, sqrt, sin, cos
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

spark = SparkSession.builder.master('local').appName('project').getOrCreate()

pre = '--- hello ---'
sname = '--- hello ---'

# пользовательская функция расчёта расстояния между координатами
udf_distance = F.udf(lambda x1, y1, x2, y2: distance(
    x1, y1, x2, y2), DoubleType())

def distance(*args):
    x1, y1, x2, y2 = map(lambda x: radians(x), args)
    return 2 * 6371 * asin(sqrt(sin((x2 - x1) / 2)**2 + cos(x1) * cos(x2) * sin((y2 - y1) / 2)**2))

# пользовательская функция определения временной зоны по названию города
udf_tz = F.udf(lambda x: tz(x), StringType())

def tz(x):
    for i in pytz.all_timezones:
        if x in i:
            return(i)
    return None


def main():

    logger = logging.getLogger(__name__)

    date = sys.argv[1]
    days_count = sys.argv[2]
    home_days = sys.argv[3]
    events_base_path = sys.argv[4]
    output_base_path = sys.argv[5]

    # Список дат
    dt = [
        f'{pre}{events_base_path}/date={(datetime.strptime(date, "%Y-%m-%d") - timedelta(days=i)).strftime("%Y-%m-%d")}' for i in range(int(days_count))]

    # создаём фрейм из нашего CSV файла с городами и координатами
    # переименовываем и преобразуем в double
    df_city = spark.read.options(header='True', delimiter=';').csv(f'{pre}{output_base_path}/geo.csv')\
        .withColumn("x", F.regexp_replace("lat", ",", ".").cast("double"))\
        .withColumn("y", F.regexp_replace("lng", ",", ".").cast("double"))\
        .selectExpr('id', 'city', 'x', 'y')

    # фрейм за указаный период
    # фильтруем по нужным событиям (subscription, message, reaction, registration)
    # убираем события без координат
    # колонка пользователь (в зависимсти от события и того в какой из колонок он есть)
    df_event_coordinate = spark.read.parquet(*dt)\
        .filter(F.col('event_type').isin(['subscription', 'message', 'reaction', 'registration']))\
        .filter(F.col('lat').isNotNull() & F.col('lon').isNotNull())\
        .withColumn('usr', F.coalesce('event.message_from', 'event.user', 'event.reaction_from'))\
        .withColumn('date', F.coalesce('event.datetime', 'event.message_ts'))

    # фрейм событий с ближайшим городом
    # кросс джоин городов и координат их центров для расчёта расстояния между кординатами события и каждым городом
    #  пользовательская функция расчёта расстояния
    # оставляем только ближайший к координатам события город
    df_event_city = df_event_coordinate\
        .crossJoin(df_city)\
        .withColumn("distance", udf_distance("lat", "lon", "x", "y"))\
        .withColumn('rn_dist', F.row_number().over(Window().partitionBy(['event', 'event_type', 'lat', 'lon', 'usr']).orderBy(F.col('distance').cast(DoubleType()))))\
        .filter(F.col('rn_dist') == 1)\

    # фрейм событий с последним ближайшим городом из которого было совершенно событие
    df_last_event_city = df_event_city\
        .withColumn('rn_dtm', F.row_number().over(Window().partitionBy(['usr']).orderBy(F.desc('date'))))\
        .filter(F.col('rn_dtm') == 1)\
        .selectExpr('usr user_id', 'city as act_city', 'date')

    #"актуальный город"
    df_act_city = df_last_event_city\
        .selectExpr('user_id', 'act_city')

    # только последнее событие для города (в случае если придётся менять логику расчёта)
    last_once_city = df_event_city\
        .selectExpr('usr', 'city', 'date')\
        .withColumn('dt', F.date_trunc("day", F.col("date").cast(DateType())))\
        .withColumn("rn", F.row_number().over(Window().partitionBy(['usr', 'city', 'dt']).orderBy(F.desc('date'))))\
        .where('rn == 1')\
        .drop('rn')

    # город в котором пользователь был 27 дней подряд
    df_home = last_once_city\
        .withColumn('rn', F.row_number().over(Window().partitionBy(['usr', 'city']).orderBy('dt')))\
        .withColumn('date_sub', F.date_sub(last_once_city.dt, F.col('rn')))\
        .withColumn('count', F.count('*').over(Window().partitionBy(['usr', 'city', 'date_sub'])))\
        .filter(F.col('count') >= home_days)\
        .withColumn('rn_last', F.row_number().over(Window().partitionBy(['usr']).orderBy(F.desc('date'))))\
        .filter(F.col('rn_last') == 1)\
        .selectExpr('usr user_id', 'city')

    # актуальный и домашний город
    act_home_city = df_act_city.join(df_home, 'user_id', 'left')\
        .selectExpr('user_id', 'act_city', 'city home_city')

    ################### количество посещенных городов ###################
    # оставляем если следующий город не такой же
    # джоином вычитаем события произошедшие из домашнего города
    # считаем сумму чеков
    df_travel_count = df_event_city\
        .selectExpr('usr user_id', 'city', 'date')\
        .na.drop()\
        .withColumn('lead', F.lead('city').over(Window().partitionBy('user_id').orderBy('date')))\
        .withColumn('chk', F.when(F.col('city') == F.col('lead'), 0).otherwise(1))\
        .filter('chk == 1')\
        .join(df_home, on=['user_id', 'city'], how='anti')\
        .withColumn('travel_count', F.sum('chk').over(Window().partitionBy('user_id')))

    # массив городов в порядке посещения
    df_travel_array = df_travel_count\
        .withColumn('travel_array', F.collect_list("city").over(Window().partitionBy('user_id').orderBy('date').rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing)))\
        .drop('city', 'date', 'lead', 'chk')\
        .dropDuplicates()

    # локальное время
    df_local_time = df_last_event_city\
        .join(df_travel_array, 'user_id', 'left')

    # Селф джойн для определения ближайшего города для которого в справочнике есть таймзона
    df_city_tz = df_city.alias('c1')\
        .withColumn('tz', udf_tz('c1.city'))\
        .where('tz is not Null')\
        .crossJoin(df_city.alias('c2'))\
        .withColumn("distance", udf_distance("c1.x", "c1.y", "c2.x", "c2.y"))\
        .withColumn('rn_dist', F.row_number().over(Window().partitionBy(['c1.city']).orderBy(F.col('distance').cast(DoubleType()))))\
        .filter(F.col('rn_dist') == 1)\
        .selectExpr('c1.id', 'c2.city', 'c1.city city_tz', 'tz')

    # Джойним ближайшие таймзонные города и определяем временную зонну по городу с помощью пользовательской функции
        #! если ставим city_tz то находим ближайший таймзонный город
        #! если вместо city_tz ставим act_city, то при nulls будет подставляеться таймзона столицы
    vitrina1 = df_local_time\
        .join(df_city_tz, df_local_time.act_city == df_city_tz.city)\
        .withColumn('local_time', F.from_utc_timestamp('date', F.coalesce(udf_tz('city_tz'), F.lit('Australia/Canberra'))))\
        .join(df_home.selectExpr('user_id', 'city home_city'), 'user_id', 'left')\
        .selectExpr('user_id', 'act_city', 'home_city', 'travel_count', 'travel_array', 'local_time')\


    # записываем
    vitrina1\
        .write\
        .mode('overwrite')\
        .parquet(f'{pre}{output_base_path}/vitrina1')

# .partitionBy('user_id')\

    #####################################################

    # количество посещений на неделю и за месяц
    df_zone = df_event_city\
        .selectExpr('date', 'event_type', 'id', 'city')\
        .na.drop()\
        .withColumn('month', F.month('date'))\
        .withColumn('week', F.weekofyear('date')) \
        .withColumn('week_event_count', F.count('*').over(Window().partitionBy('city', 'month', 'week', 'event_type')))\
        .withColumn('month_event_count', F.count('*').over(Window().partitionBy('city', 'month', 'event_type')))\
        .selectExpr('event_type', 'id', 'city', 'month', 'week', 'week_event_count', 'month_event_count')\
        .dropDuplicates()\

    # Разворачиваем по типу события "event_type" за неделю
    df_sum_week = df_zone\
        .groupBy('id', 'city', 'month', 'week').pivot('event_type', ('message', 'reaction', 'subscription', 'registration')).agg(F.first('week_event_count'))\
        .selectExpr('id', 'city', 'month', 'week', 'message week_message', ' reaction week_reaction', 'subscription week_subscription', 'registration week_user')

    # Разворачиваем по типу события "event_type" за месяц
    df_sum_month = df_zone\
        .groupBy('id', 'city', 'month').pivot('event_type', ('message', 'reaction', 'subscription', 'registration')).agg(F.first('month_event_count'))\
        .selectExpr('id ', 'city', 'month', 'message month_message', 'reaction month_reaction', 'subscription month_subscription', 'registration month_user')

    # Собираем витрину
    # Джойним количесво за неделю с количеством за месяц
    vitrina2 = df_sum_week.join(df_sum_month, (['id', 'city', 'month']), 'inner')\
        .selectExpr('month ', 'week', 'id zone_id', 'week_message', ' week_reaction', 'week_subscription', 'week_user', 'month_message', 'month_reaction', 'month_subscription', 'month_user')

    # записываем
    vitrina2\
        .write\
        .mode('overwrite')\
        .partitionBy('month', 'week')\
        .parquet(f'{pre}{output_base_path}/vitrina2')

    #####################################################

    # фрейм подписок где есть информация о канале
    subscription_channel = spark.read.parquet(*dt)\
        .where('event_type == "subscription" and event.subscription_channel is not null')\
        .selectExpr('event.user', 'event.subscription_channel')\
        .distinct()

    # self join для выборки уникальных пар подписанных на одинаковые каналы пользователей
    common_chanel = subscription_channel.alias('df1').join(subscription_channel.alias('df2'), 'subscription_channel', 'inner')\
        .selectExpr('df1.user user_left', 'df2.user user_right')\
        .where('user_left < user_right')\
        .distinct()

    # выбираем уникальные пары пользователей которые переписывались
    comm_users = spark.read.parquet(*dt)\
        .where('event_type == "message" and event.message_from is not null and event.message_to is not null')\
        .selectExpr('event.message_from', 'event.message_to')\
        .withColumn("user_left", F.when(F.col("message_from") > F.col("message_to"), F.col("message_to")).otherwise(F.col("message_from")))\
        .withColumn("user_right", F.when(F.col("message_from") < F.col("message_to"), F.col("message_to")).otherwise(F.col("message_from")))\
        .select("user_left", "user_right")\
        .distinct()

    # выбираем последнее событие для пользователя с наличием координат и времени
    coordinates = spark.read.parquet(*dt)\
        .selectExpr('event.user user_id', 'lat', 'lon', 'event.datetime')\
        .where('lat is not null and lon is not null and user_id is not null')\
        .withColumn('last_date', F.max('datetime').over(Window().partitionBy(['user_id'])
    .orderBy(F.desc('datetime'))))\
        .where('datetime == last_date')\
        .distinct()

    # из пользователей подписанных на один канал вычитаем пользователей которые переписывались
    result = common_chanel.join(comm_users, ['user_left', 'user_right'], 'anti')\
        .join(coordinates, F.col('user_left') == F.col('user_id'), 'inner')\
        .drop(coordinates.user_id)\
        .join(coordinates.selectExpr('user_id', 'lat x2', 'lon y2', 'datetime', 'last_date'), F.col('user_right') == F.col('user_id'), 'inner')\
        .selectExpr('user_left', 'user_right', 'lat x1', 'lon y1', 'user_id', 'x2', 'y2')

    # .selectExpr('user_left', 'user_right', 'common_chanel.lat x1', 'common_chanel.lon y1', 'common_chanel.datetime', 'common_chanel.last_date', 'user_id', 'comm_users.lat x2', 'comm_users.lon y2', 'comm_users.datetime', 'comm_users.last_date')\
        # .toDF('user_left', 'user_right', 'x1', 'y1', 'datetime', 'last_date', 'user_id', 'x2', 'y2', 'datetime', 'last_date')

    # вычисляем расстояние между пользователями и оставляем если меньше 1 километра
    result = result\
        .withColumn("distance", udf_distance("x1", "y1", "x2", "y2"))\
        .where('distance <= 1')

    # Собираем витрину определяем ближайший город
        #! если ставим city_tz то находим ближайший таймзонный город
        #! если вместо city_tz ставим act_city, то при nulls будет подставляеться таймзона столицы
    vitrina3 = result.crossJoin(df_city)\
        .withColumn("distance", udf_distance("x1", "y1", "x", "y"))\
        .withColumn('min', F.min('distance').over(Window().partitionBy(['user_left', 'user_right'])))\
        .where('distance == min')\
        .selectExpr('user_left', 'user_right', 'current_date() processed_dttm', 'id zone_id', 'city', 'current_timestamp() local_time')\
        .join(df_city_tz, 'city')\
        .withColumn('local_time', F.from_utc_timestamp('local_time', F.coalesce(udf_tz('city_tz'), F.lit('Australia/Canberra'))))\
        .selectExpr('user_left', 'user_right', 'processed_dttm', 'zone_id', 'local_time')

    # записываем
    vitrina3\
        .write\
        .mode('overwrite')\
        .parquet(f'{pre}{output_base_path}/vitrina3')

# .partitionBy('user_left', 'user_right')\


    logging.info('Vitrin1')
    spark.read.parquet(f'{pre}{output_base_path}/vitrina1')\
        .show()

    logging.info('Vitrin2')
    spark.read.parquet(f'{pre}{output_base_path}/vitrina2')\
    .show()

    logging.info('Vitrin3')
    spark.read.parquet(f'{pre}{output_base_path}/vitrina3')\
    .show()

if __name__ == "__main__":
        main()
