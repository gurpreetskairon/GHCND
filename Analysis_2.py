#################################################################################################
## Analysis Q2a : Write a Spark function that computes the geographical distance between two   ##
##                stations using their latitude and longitude as arguments. You can test this  ##
##                function by using CROSS JOIN on a small subset of stations to generate a     ##
##                table with two stations in each row.                                         ##
#################################################################################################

from math import radians, cos, sin, asin, sqrt

def get_distance(longitude_a, latitude_a, longitude_b, latitude_b):
    # Transform to radians
    longitude_a, latitude_a, longitude_b, latitude_b = map(radians, [longitude_a,  latitude_a, longitude_b, latitude_b])
    dist_longitude = longitude_b - longitude_a
    dist_latitude = latitude_b - latitude_a
    
    # Calculate area
    area = sin(dist_latitude/2)**2 + cos(latitude_a) * cos(latitude_b) * sin(dist_longitude/2)**2
    
    # Calculate the central angle
    central_angle = 2 * asin(sqrt(area))
    
    # Calculate Distance
    distance = central_angle * 6371
    
    return abs(round(distance, 2))

udf_get_distance = F.udf(get_distance)


NZ_stations_data = stations_with_countryid.select('ID', 'NAME', 'LATITUDE', 'LONGITUDE').filter(F.col('CODE') == 'NZ')
NZ_station_pairs = NZ_stations_data.crossJoin(NZ_stations_data)\
  .toDF('ID_A', 'NAME_A', 'LATITUDE_A', 'LONGITUDE_A', 'ID_B', 'NAME_B', 'LATITUDE_B', 'LONGITUDE_B')

# remove duplicate rows
NZ_station_pairs = NZ_station_pairs.filter(NZ_station_pairs.ID_A != NZ_station_pairs.ID_B)

# calculate the distance
pairs_distance = NZ_station_pairs.withColumn('DISTANCE', udf_get_distance(
                 NZ_station_pairs.LONGITUDE_A, NZ_station_pairs.LATITUDE_A, 
                 NZ_station_pairs.LONGITUDE_B, NZ_station_pairs.LATITUDE_B)
            .cast(DoubleType()))

pairs_distance.sort('DISTANCE').show()

''' Output
+-----------+-------------------+----------+-----------+-----------+-------------------+----------+-----------+--------+
|       ID_A|             NAME_A|LATITUDE_A|LONGITUDE_A|       ID_B|             NAME_B|LATITUDE_B|LONGITUDE_B|DISTANCE|
+-----------+-------------------+----------+-----------+-----------+-------------------+----------+-----------+--------+
|NZ000093417|    PARAPARAUMU AWS|     -40.9|    174.983|NZM00093439|WELLINGTON AERO AWS|   -41.333|      174.8|   50.53|
|NZM00093439|WELLINGTON AERO AWS|   -41.333|      174.8|NZ000093417|    PARAPARAUMU AWS|     -40.9|    174.983|   50.53|
|NZM00093439|WELLINGTON AERO AWS|   -41.333|      174.8|NZM00093678|           KAIKOURA|   -42.417|      173.7|  151.07|
|NZM00093678|           KAIKOURA|   -42.417|      173.7|NZM00093439|WELLINGTON AERO AWS|   -41.333|      174.8|  151.07|
|NZ000936150| HOKITIKA AERODROME|   -42.717|    170.983|NZM00093781|  CHRISTCHURCH INTL|   -43.489|    172.532|  152.26|
|NZM00093781|  CHRISTCHURCH INTL|   -43.489|    172.532|NZ000936150| HOKITIKA AERODROME|   -42.717|    170.983|  152.26|
|NZM00093678|           KAIKOURA|   -42.417|      173.7|NZM00093781|  CHRISTCHURCH INTL|   -43.489|    172.532|  152.46|
|NZM00093781|  CHRISTCHURCH INTL|   -43.489|    172.532|NZM00093678|           KAIKOURA|   -42.417|      173.7|  152.46|
|NZ000093417|    PARAPARAUMU AWS|     -40.9|    174.983|NZM00093678|           KAIKOURA|   -42.417|      173.7|  199.53|
|NZM00093678|           KAIKOURA|   -42.417|      173.7|NZ000093417|    PARAPARAUMU AWS|     -40.9|    174.983|  199.53|
|NZ000937470|         TARA HILLS|   -44.517|      169.9|NZ000936150| HOKITIKA AERODROME|   -42.717|    170.983|  218.31|
|NZ000936150| HOKITIKA AERODROME|   -42.717|    170.983|NZ000937470|         TARA HILLS|   -44.517|      169.9|  218.31|
|NZ000093417|    PARAPARAUMU AWS|     -40.9|    174.983|NZ000933090|   NEW PLYMOUTH AWS|   -39.017|    174.183|   220.2|
|NZ000933090|   NEW PLYMOUTH AWS|   -39.017|    174.183|NZ000093417|    PARAPARAUMU AWS|     -40.9|    174.983|   220.2|
|NZ000936150| HOKITIKA AERODROME|   -42.717|    170.983|NZM00093678|           KAIKOURA|   -42.417|      173.7|  224.98|
|NZM00093678|           KAIKOURA|   -42.417|      173.7|NZ000936150| HOKITIKA AERODROME|   -42.717|    170.983|  224.98|
|NZM00093110|  AUCKLAND AERO AWS|     -37.0|      174.8|NZ000933090|   NEW PLYMOUTH AWS|   -39.017|    174.183|   230.7|
|NZ000933090|   NEW PLYMOUTH AWS|   -39.017|    174.183|NZM00093110|  AUCKLAND AERO AWS|     -37.0|      174.8|   230.7|
|NZ000937470|         TARA HILLS|   -44.517|      169.9|NZM00093781|  CHRISTCHURCH INTL|   -43.489|    172.532|  239.53|
|NZM00093781|  CHRISTCHURCH INTL|   -43.489|    172.532|NZ000937470|         TARA HILLS|   -44.517|      169.9|  239.53|
+-----------+-------------------+----------+-----------+-----------+-------------------+----------+-----------+--------+
only showing top 20 rows
'''

# Save the results to an output file of parquet format
pairs_distance.write.mode("overwrite").parquet('./ScalableAssignment1/NZ_station_pairs_distance.parquet')

# hdfs dfs -ls ./ScalableAssignment1
''' Output
Found 7 items
drwxr-xr-x   - gsi58 gsi58          0 2020-09-09 14:23 ScalableAssignment1/NZ_station_pairs_distance.parquet
drwxr-xr-x   - gsi58 gsi58          0 2020-09-08 18:18 ScalableAssignment1/average_rainfall.csv
drwxr-xr-x   - gsi58 gsi58          0 2020-09-06 21:01 ScalableAssignment1/countries_data_with_stations_count.parquet
drwxr-xr-x   - gsi58 gsi58          0 2020-09-08 17:42 ScalableAssignment1/daily_all_nz_T_elements.csv
drwxr-xr-x   - gsi58 gsi58          0 2020-09-08 16:36 ScalableAssignment1/daily_all_nz_T_elements.parquet
drwxr-xr-x   - gsi58 gsi58          0 2020-09-06 21:44 ScalableAssignment1/states_data_with_stations_count.parquet
drwxr-xr-x   - gsi58 gsi58          0 2020-09-09 10:04 ScalableAssignment1/stations_inventory.parquet
'''

df = spark.read.load('./ScalableAssignment1/NZ_station_pairs_distance.parquet')
df.show()

''' Output
+-----------+------------------+----------+-----------+-----------+-------------------+----------+-----------+--------+
|       ID_A|            NAME_A|LATITUDE_A|LONGITUDE_A|       ID_B|             NAME_B|LATITUDE_B|LONGITUDE_B|DISTANCE|
+-----------+------------------+----------+-----------+-----------+-------------------+----------+-----------+--------+
|NZ000093012|           KAITAIA|     -35.1|    173.267|NZ000093292| GISBORNE AERODROME|    -38.65|    177.983|  575.85|
|NZ000093012|           KAITAIA|     -35.1|    173.267|NZ000093417|    PARAPARAUMU AWS|     -40.9|    174.983|  662.18|
|NZ000093012|           KAITAIA|     -35.1|    173.267|NZ000093844|INVERCARGILL AIRPOR|   -46.417|    168.333| 1324.53|
|NZ000093012|           KAITAIA|     -35.1|    173.267|NZ000093994| RAOUL ISL/KERMADEC|    -29.25|   -177.917| 1053.53|
|NZ000093012|           KAITAIA|     -35.1|    173.267|NZ000933090|   NEW PLYMOUTH AWS|   -39.017|    174.183|  443.06|
|NZ000093012|           KAITAIA|     -35.1|    173.267|NZ000936150| HOKITIKA AERODROME|   -42.717|    170.983|  869.62|
|NZ000093012|           KAITAIA|     -35.1|    173.267|NZ000937470|         TARA HILLS|   -44.517|      169.9| 1085.63|
|NZ000093012|           KAITAIA|     -35.1|    173.267|NZ000939450|CAMPBELL ISLAND AWS|    -52.55|    169.167| 1967.22|
|NZ000093012|           KAITAIA|     -35.1|    173.267|NZ000939870|CHATHAM ISLANDS AWS|    -43.95|   -176.567| 1312.73|
|NZ000093012|           KAITAIA|     -35.1|    173.267|NZM00093110|  AUCKLAND AERO AWS|     -37.0|      174.8|  252.24|
|NZ000093012|           KAITAIA|     -35.1|    173.267|NZM00093439|WELLINGTON AERO AWS|   -41.333|      174.8|  705.86|
|NZ000093012|           KAITAIA|     -35.1|    173.267|NZM00093678|           KAIKOURA|   -42.417|      173.7|  814.48|
|NZ000093012|           KAITAIA|     -35.1|    173.267|NZM00093781|  CHRISTCHURCH INTL|   -43.489|    172.532|  934.94|
|NZ000093012|           KAITAIA|     -35.1|    173.267|NZM00093929| ENDERBY ISLAND AWS|   -50.483|      166.3| 1800.52|
|NZ000093292|GISBORNE AERODROME|    -38.65|    177.983|NZ000093012|            KAITAIA|     -35.1|    173.267|  575.85|
|NZ000093292|GISBORNE AERODROME|    -38.65|    177.983|NZ000093417|    PARAPARAUMU AWS|     -40.9|    174.983|  358.18|
|NZ000093292|GISBORNE AERODROME|    -38.65|    177.983|NZ000093844|INVERCARGILL AIRPOR|   -46.417|    168.333| 1169.21|
|NZ000093292|GISBORNE AERODROME|    -38.65|    177.983|NZ000093994| RAOUL ISL/KERMADEC|    -29.25|   -177.917| 1111.19|
|NZ000093292|GISBORNE AERODROME|    -38.65|    177.983|NZ000933090|   NEW PLYMOUTH AWS|   -39.017|    174.183|  331.64|
|NZ000093292|GISBORNE AERODROME|    -38.65|    177.983|NZ000936150| HOKITIKA AERODROME|   -42.717|    170.983|  743.14|
+-----------+------------------+----------+-----------+-----------+-------------------+----------+-----------+--------+
only showing top 20 rows
'''