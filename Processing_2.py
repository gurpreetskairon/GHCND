# Python and pyspark modules required

import sys
from pyspark import SparkContext
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import * 

# Required to allow the file to be submitted and run using spark-submit instead
# of using pyspark interactively
spark = SparkSession.builder.getOrCreate()
sc = SparkContext.getOrCreate()

#################################################################################################
## Processing Q2a: Define Schemas for each of daily, stations,                                 ##
##                 states, countries, and inventory                                            ##
#################################################################################################
## daily schema                                                                                ##
daily_schema = StructType([
  StructField("ID", StringType(), True),
  StructField("DATE", StringType(), True),
  StructField("ELEMENT", StringType(), True),
  StructField("VALUE", DoubleType(), True),
  StructField("MEASUREMENT_FLAG", StringType(), True),
  StructField("QUALITY_FLAG", StringType(), True),
  StructField("SOURCE_FLAG", StringType(), True),
  StructField("OBSERVATION_TIME", StringType(), True),
])

## stations schema                                                                            ##
stations_schema = StructType([
    StructField("ID", StringType(), True),
    StructField("LATITUDE", DoubleType(), True),
    StructField("LONGITUDE", DoubleType(), True),
    StructField("ELEVATION", DoubleType(), True),
    StructField("STATE", StringType(), True),
    StructField("NAME", StringType(), True),
    StructField("GSN FLAG", StringType(), True),
    StructField("HCN/CRN FLAG", StringType(), True),
    StructField("WMO ID", StringType(), True),
])

## states schema                                                                              ##
states_schema = StructType([
  StructField("CODE", StringType(), True),
  StructField("NAME", StringType(), True),
])

## countries schema                                                                           ##
countries_schema = StructType([
  StructField("CODE", StringType(), True),
  StructField("NAME", StringType(), True),
])

## inventory schema                                                                           ##
inventory_schema = StructType([
  StructField("ID", StringType(), True),
  StructField("LATITUDE", DoubleType(), True),
  StructField("LATITUDE", DoubleType(), True),
  StructField("ELEMENT", StringType(), True),
  StructField("FIRSTYEAR", IntegerType(), True),
  StructField("LASTYEAR", IntegerType(), True),
])


#################################################################################################
## Processing Q2b: Load 1000 rows of hdfs:///data/ghcnd/daily/2020.csv.gz                      ##
#################################################################################################
daily = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .schema(daily_schema)
    .load("hdfs:////data/ghcnd/daily/2020.csv.gz")
    .limit(1000)
)
daily.cache()
daily.show(10, False)
daily.count()

''' Output
+-----------+--------+-------+-----+----------------+------------+-----------+----------------+
|ID         |DATE    |ELEMENT|VALUE|MEASUREMENT_FLAG|QUALITY_FLAG|SOURCE_FLAG|OBSERVATION_TIME|
+-----------+--------+-------+-----+----------------+------------+-----------+----------------+
|US1FLSL0019|20200101|PRCP   |0.0  |null            |null        |N          |null            |
|US1FLSL0019|20200101|SNOW   |0.0  |null            |null        |N          |null            |
|US1NVNY0012|20200101|PRCP   |0.0  |null            |null        |N          |null            |
|US1NVNY0012|20200101|SNOW   |0.0  |null            |null        |N          |null            |
|US1ILWM0012|20200101|PRCP   |0.0  |null            |null        |N          |null            |
|USS0018D08S|20200101|TMAX   |46.0 |null            |null        |T          |null            |
|USS0018D08S|20200101|TMIN   |6.0  |null            |null        |T          |null            |
|USS0018D08S|20200101|TOBS   |6.0  |null            |null        |T          |null            |
|USS0018D08S|20200101|PRCP   |76.0 |null            |null        |T          |null            |
|USS0018D08S|20200101|SNWD   |0.0  |null            |null        |T          |null            |
+-----------+--------+-------+-----+----------------+------------+-----------+----------------+
only showing top 10 rows
1000
'''

#################################################################################################
## Processing Q2c: Load each of stations, states, countries, and inventory into Spark as well  ##
#################################################################################################

## Load the Stations data                                                                      ##
stations = (
    spark.read.format("text")
    .text("hdfs:///data/ghcnd/stations")
) 
stations_data = stations.select(
    F.trim(F.substring(F.col('value'),1, 11)).alias('ID').cast(StringType()),
    F.trim(F.substring(F.col('value'),13, 8)).alias('LATITUDE').cast(StringType()),
    F.trim(F.substring(F.col('value'),22, 9)).alias('LONGITUDE').cast(StringType()),
    F.trim(F.substring(F.col('value'),32, 6)).alias('ELEVATION').cast(StringType()),
    F.trim(F.substring(F.col('value'),39, 2)).alias('STATE').cast(StringType()),
    F.trim(F.substring(F.col('value'),42, 30)).alias('NAME').cast(StringType()),
    F.trim(F.substring(F.col('value'),73, 3)).alias('GSN_FLAG').cast(StringType()),
    F.trim(F.substring(F.col('value'),77, 3)).alias('HCN/CRN_FLAG').cast(StringType()),
    F.trim(F.substring(F.col('value'),81, 5)).alias('WMO_ID').cast(StringType())
)
stations_data.cache()
stations_data.show()
stations_data.registerTempTable('stations_tbl')

''' Output
+-----------+--------+---------+---------+-----+--------------------+--------+------------+------+
|         ID|LATITUDE|LONGITUDE|ELEVATION|STATE|                NAME|GSN_FLAG|HCN/CRN_FLAG|WMO_ID|
+-----------+--------+---------+---------+-----+--------------------+--------+------------+------+
|ACW00011604| 17.1167| -61.7833|     10.1|     |ST JOHNS COOLIDGE...|        |            |      |
|ACW00011647| 17.1333| -61.7833|     19.2|     |            ST JOHNS|        |            |      |
|AE000041196| 25.3330|  55.5170|     34.0|     | SHARJAH INTER. AIRP|     GSN|            | 41196|
|AEM00041194| 25.2550|  55.3640|     10.4|     |          DUBAI INTL|        |            | 41194|
|AEM00041217| 24.4330|  54.6510|     26.8|     |      ABU DHABI INTL|        |            | 41217|
|AEM00041218| 24.2620|  55.6090|    264.9|     |         AL AIN INTL|        |            | 41218|
|AF000040930| 35.3170|  69.0170|   3366.0|     |        NORTH-SALANG|     GSN|            | 40930|
|AFM00040938| 34.2100|  62.2280|    977.2|     |               HERAT|        |            | 40938|
|AFM00040948| 34.5660|  69.2120|   1791.3|     |          KABUL INTL|        |            | 40948|
|AFM00040990| 31.5000|  65.8500|   1010.0|     |    KANDAHAR AIRPORT|        |            | 40990|
|AG000060390| 36.7167|   3.2500|     24.0|     |  ALGER-DAR EL BEIDA|     GSN|            | 60390|
|AG000060590| 30.5667|   2.8667|    397.0|     |            EL-GOLEA|     GSN|            | 60590|
|AG000060611| 28.0500|   9.6331|    561.0|     |           IN-AMENAS|     GSN|            | 60611|
|AG000060680| 22.8000|   5.4331|   1362.0|     |         TAMANRASSET|     GSN|            | 60680|
|AGE00135039| 35.7297|   0.6500|     50.0|     |ORAN-HOPITAL MILI...|        |            |      |
|AGE00147704| 36.9700|   7.7900|    161.0|     | ANNABA-CAP DE GARDE|        |            |      |
|AGE00147705| 36.7800|   3.0700|     59.0|     |ALGIERS-VILLE/UNI...|        |            |      |
|AGE00147706| 36.8000|   3.0300|    344.0|     |   ALGIERS-BOUZAREAH|        |            |      |
|AGE00147707| 36.8000|   3.0400|     38.0|     |  ALGIERS-CAP CAXINE|        |            |      |
|AGE00147708| 36.7200|   4.0500|    222.0|     |          TIZI OUZOU|        |            | 60395|
+-----------+--------+---------+---------+-----+--------------------+--------+------------+------+
only showing top 20 rows
'''

## Load the states data                                                                       ##
states = (
    spark.read.format("text")
    .text("hdfs:////data/ghcnd/states")
)
states_data = states.select(
    F.trim(F.substring(F.col('value'),1, 2)).alias('CODE').cast(StringType()),
    F.trim(F.substring(F.col('value'),4, 47)).alias('NAME').cast(StringType())
)
states.cache()
states_data.show()
states_data.registerTempTable('states_tbl')

''' Output
+----+--------------------+
|CODE|                NAME|
+----+--------------------+
|  AB|             ALBERTA|
|  AK|              ALASKA|
|  AL|             ALABAMA|
|  AR|            ARKANSAS|
|  AS|      AMERICAN SAMOA|
|  AZ|             ARIZONA|
|  BC|    BRITISH COLUMBIA|
|  CA|          CALIFORNIA|
|  CO|            COLORADO|
|  CT|         CONNECTICUT|
|  DC|DISTRICT OF COLUMBIA|
|  DE|            DELAWARE|
|  FL|             FLORIDA|
|  FM|          MICRONESIA|
|  GA|             GEORGIA|
|  GU|                GUAM|
|  HI|              HAWAII|
|  IA|                IOWA|
|  ID|               IDAHO|
|  IL|            ILLINOIS|
+----+--------------------+
only showing top 20 rows
'''

## Load the countries data                                                                    ##                    
countries = (
    spark.read.format("text")
    .load("hdfs:///data/ghcnd/countries")
)

countries_data = countries.select(
    F.trim(F.substring(F.col('value'), 1, 2)).alias('CODE').cast(StringType()),
    F.trim(F.substring(F.col('value'), 4, 47)).alias('NAME').cast(StringType())
)

countries_data.cache()
countries_data.show()
countries_data.registerTempTable('countries_tbl')

''' Output
+----+--------------------+
|CODE|                NAME|
+----+--------------------+
|  AC| Antigua and Barbuda|
|  AE|United Arab Emirates|
|  AF|         Afghanistan|
|  AG|             Algeria|
|  AJ|          Azerbaijan|
|  AL|             Albania|
|  AM|             Armenia|
|  AO|              Angola|
|  AQ|American Samoa [U...|
|  AR|           Argentina|
|  AS|           Australia|
|  AU|             Austria|
|  AY|          Antarctica|
|  BA|             Bahrain|
|  BB|            Barbados|
|  BC|            Botswana|
|  BD|Bermuda [United K...|
|  BE|             Belgium|
|  BF|        Bahamas, The|
|  BG|          Bangladesh|
+----+--------------------+
only showing top 20 rows
'''

## Load inventory data                                                                         ##
inventory = (
    spark.read.format("text")
    .load("hdfs:////data/ghcnd/inventory")
)
inventory_data = inventory.select(
    F.trim(F.substring(F.col('value'),1, 11)).alias('ID').cast(StringType()),
    F.trim(F.substring(F.col('value'),13, 8)).alias('LATITUDE').cast(DoubleType()),
    F.trim(F.substring(F.col('value'),22, 9)).alias('LONGITUDE').cast(DoubleType()),
    F.trim(F.substring(F.col('value'),32, 4)).alias('ELEMENT').cast(StringType()),
    F.trim(F.substring(F.col('value'),37, 4)).alias('FIRSTYEAR').cast(IntegerType()),
    F.trim(F.substring(F.col('value'),42, 4)).alias('LASTYEAR').cast(IntegerType())    
)
inventory_data.cache()
inventory_data.show()
inventory_data.registerTempTable('inventory_tbl')

''' Output
+-----------+--------+---------+-------+---------+--------+
|         ID|LATITUDE|LONGITUDE|ELEMENT|FIRSTYEAR|LASTYEAR|
+-----------+--------+---------+-------+---------+--------+
|ACW00011604| 17.1167| -61.7833|   TMAX|     1949|    1949|
|ACW00011604| 17.1167| -61.7833|   TMIN|     1949|    1949|
|ACW00011604| 17.1167| -61.7833|   PRCP|     1949|    1949|
|ACW00011604| 17.1167| -61.7833|   SNOW|     1949|    1949|
|ACW00011604| 17.1167| -61.7833|   SNWD|     1949|    1949|
|ACW00011604| 17.1167| -61.7833|   PGTM|     1949|    1949|
|ACW00011604| 17.1167| -61.7833|   WDFG|     1949|    1949|
|ACW00011604| 17.1167| -61.7833|   WSFG|     1949|    1949|
|ACW00011604| 17.1167| -61.7833|   WT03|     1949|    1949|
|ACW00011604| 17.1167| -61.7833|   WT08|     1949|    1949|
|ACW00011604| 17.1167| -61.7833|   WT16|     1949|    1949|
|ACW00011647| 17.1333| -61.7833|   TMAX|     1961|    1961|
|ACW00011647| 17.1333| -61.7833|   TMIN|     1961|    1961|
|ACW00011647| 17.1333| -61.7833|   PRCP|     1957|    1970|
|ACW00011647| 17.1333| -61.7833|   SNOW|     1957|    1970|
|ACW00011647| 17.1333| -61.7833|   SNWD|     1957|    1970|
|ACW00011647| 17.1333| -61.7833|   WT03|     1961|    1961|
|ACW00011647| 17.1333| -61.7833|   WT16|     1961|    1966|
|AE000041196|  25.333|   55.517|   TMAX|     1944|    2019|
|AE000041196|  25.333|   55.517|   TMIN|     1944|    2020|
+-----------+--------+---------+-------+---------+--------+
only showing top 20 rows
'''

## How many rows are in each metadata table?                                                    ##
stations_data.count()
states_data.count()
countries_data.count()
inventory_data.count()

''' Output
115081
74
219
687141
'''

## How many stations do not have a WMO ID?                                                      ##
stations_data.filter(stations_data.WMO_ID == '').count()

''' Output
106993
'''