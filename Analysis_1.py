#################################################################################################
## Analysis Q1a: How many stations are there in total?                                         ##
#################################################################################################
stations_data.count()

''' Output
115081
'''

## How many stations were active in 2000?                                                     ##
inventory_data.filter((F.col('FIRSTYEAR') <= 2000) & (F.col('LASTYEAR') >= 2000)).select('ID').distinct().count()
''' Output
32868
'''
inventory_data.filter((F.col('FIRSTYEAR') <= 2000) & (F.col('LASTYEAR') >= 2000))

## How many stations are in each of the GCOS Surface Network GSN), the US Historical          ##
## Climatology Network (HCN), and the US Climate Reference Network (CRN)? Are there           ##
## any stations that are in more than one of these networks?                                  ##
stations_data.groupBy('GSN_FLAG', 'HCN/CRN_FLAG').count().show()

''' Output
+--------+------------+------+
|GSN_FLAG|HCN/CRN_FLAG| count|
+--------+------------+------+
|     GSN|            |   977|
|     GSN|         HCN|    14|
|        |         CRN|   233|
|        |         HCN|  1204|
|        |            |112653|
+--------+------------+------+
'''


#################################################################################################
## Analysis Q1b: Count the total number of stations in each country, and store the output in   ##
##               countries using the withColumnRenamed command.                                ##
#################################################################################################
country_data = countries_data.selectExpr('CODE', 'NAME as COUNTRYNAME')
stations_country = stations_with_countryid.join(country_data, on = 'CODE', how = "left").groupBy('CODE').count()
countries_data_with_stations_count = country_data.join(stations_country, on='CODE', how = 'left')\
.withColumnRenamed('count', 'NUMBEROFSTATIONS')
countries_data_with_stations_count.sort('CODE').show(truncate = False)

''' Output
+----+------------------------------+----------------+
|CODE|COUNTRYNAME                   |NUMBEROFSTATIONS|
+----+------------------------------+----------------+
|AC  |Antigua and Barbuda           |2               |
|AE  |United Arab Emirates          |4               |
|AF  |Afghanistan                   |4               |
|AG  |Algeria                       |87              |
|AJ  |Azerbaijan                    |66              |
|AL  |Albania                       |3               |
|AM  |Armenia                       |53              |
|AO  |Angola                        |6               |
|AQ  |American Samoa [United States]|20              |
|AR  |Argentina                     |101             |
|AS  |Australia                     |17088           |
|AU  |Austria                       |13              |
|AY  |Antarctica                    |102             |
|BA  |Bahrain                       |1               |
|BB  |Barbados                      |1               |
|BC  |Botswana                      |21              |
|BD  |Bermuda [United Kingdom]      |2               |
|BE  |Belgium                       |1               |
|BF  |Bahamas, The                  |39              |
|BG  |Bangladesh                    |10              |
+----+------------------------------+----------------+
only showing top 20 rows

'''
##  save a copy of each table to your output directory.
countries_data_with_stations_count.write.mode("overwrite").parquet('./ScalableAssignment1/countries_data_with_stations_count.parquet')

''' Output
Found 2 items
drwxr-xr-x   - gsi58 gsi58          0 2020-09-06 21:01 ScalableAssignment1/countries_data_with_stations_count.parquet
drwxr-xr-x   - gsi58 gsi58          0 2020-09-06 18:29 ScalableAssignment1/stations_inventory.parquet
'''

df = spark.read.load('./ScalableAssignment1/countries_data_with_stations_count.parquet')
df.show()

''' Output
+----+------------------------------+----------------+
|CODE|COUNTRYNAME                   |NUMBEROFSTATIONS|
+----+------------------------------+----------------+
|AC  |Antigua and Barbuda           |2               |
|AE  |United Arab Emirates          |4               |
|AF  |Afghanistan                   |4               |
|AG  |Algeria                       |87              |
|AJ  |Azerbaijan                    |66              |
|AL  |Albania                       |3               |
|AM  |Armenia                       |53              |
|AO  |Angola                        |6               |
|AQ  |American Samoa [United States]|20              |
|AR  |Argentina                     |101             |
|AS  |Australia                     |17088           |
|AU  |Austria                       |13              |
|AY  |Antarctica                    |102             |
|BA  |Bahrain                       |1               |
|BB  |Barbados                      |1               |
|BC  |Botswana                      |21              |
|BD  |Bermuda [United Kingdom]      |2               |
|BE  |Belgium                       |1               |
|BF  |Bahamas, The                  |39              |
|BG  |Bangladesh                    |10              |
+----+------------------------------+----------------+
only showing top 20 rows
'''

## Do the same for states
states_names = states_data.selectExpr('CODE', 'NAME as STATENAME')
stations_states = stations_data.join(states_names, stations_data.STATE == states_names.CODE, how = 'left')
states_data_with_stations_count = stations_states.groupBy('STATE', 'STATENAME').count().withColumnRenamed('count', 'NUMBEROFSTATIONS')
states_data_with_stations_count.sort('STATE').show(truncate = False)

''' Output
+-----+--------------------+----------------+
|STATE|STATENAME           |NUMBEROFSTATIONS|
+-----+--------------------+----------------+
|     |null                |43977           |
|AB   |ALBERTA             |1420            |
|AK   |ALASKA              |986             |
|AL   |ALABAMA             |979             |
|AR   |ARKANSAS            |864             |
|AS   |AMERICAN SAMOA      |20              |
|AZ   |ARIZONA             |1453            |
|BC   |BRITISH COLUMBIA    |1683            |
|BH   |null                |33              |
|CA   |CALIFORNIA          |2798            |
|CO   |COLORADO            |4176            |
|CT   |CONNECTICUT         |310             |
|DC   |DISTRICT OF COLUMBIA|14              |
|DE   |DELAWARE            |114             |
|FL   |FLORIDA             |1744            |
|FM   |MICRONESIA          |38              |
|GA   |GEORGIA             |1210            |
|GU   |GUAM                |20              |
|HI   |HAWAII              |733             |
|IA   |IOWA                |864             |
+-----+--------------------+----------------+
only showing top 20 rows
'''


##  save a copy of each table to your output directory.
states_data_with_stations_count.write.mode("overwrite").parquet('./ScalableAssignment1/states_data_with_stations_count.parquet')

''' Output
Found 3 items
drwxr-xr-x   - gsi58 gsi58          0 2020-09-06 21:01 ScalableAssignment1/countries_data_with_stations_count.parquet
drwxr-xr-x   - gsi58 gsi58          0 2020-09-06 21:44 ScalableAssignment1/states_data_with_stations_count.parquet
drwxr-xr-x   - gsi58 gsi58          0 2020-09-06 18:29 ScalableAssignment1/stations_inventory.parquet
'''

df = spark.read.load('./ScalableAssignment1/states_data_with_stations_count.parquet')
df.show()

''' Output
+-----+--------------------+----------------+
|STATE|STATENAME           |NUMBEROFSTATIONS|
+-----+--------------------+----------------+
|     |null                |43977           |
|AB   |ALBERTA             |1420            |
|AK   |ALASKA              |986             |
|AL   |ALABAMA             |979             |
|AR   |ARKANSAS            |864             |
|AS   |AMERICAN SAMOA      |20              |
|AZ   |ARIZONA             |1453            |
|BC   |BRITISH COLUMBIA    |1683            |
|BH   |null                |33              |
|CA   |CALIFORNIA          |2798            |
|CO   |COLORADO            |4176            |
|CT   |CONNECTICUT         |310             |
|DC   |DISTRICT OF COLUMBIA|14              |
|DE   |DELAWARE            |114             |
|FL   |FLORIDA             |1744            |
|FM   |MICRONESIA          |38              |
|GA   |GEORGIA             |1210            |
|GU   |GUAM                |20              |
|HI   |HAWAII              |733             |
|IA   |IOWA                |864             |
+-----+--------------------+----------------+
only showing top 20 rows
'''


#################################################################################################
## Analysis Q1c: How many stations are there in the Southern Hemisphere only?                  ##
#################################################################################################
'''setHemisphere = lambda cond: F.when(cond, 'Southern Hemisphere').otherwise('Northen Hemisphere')
stations_data.withColumn('HEMISPHERE', setHemisphere(F.col('LATITUDE') < 0)).groupBy('HEMISPHERE')\
             .count().select(F.col('HEMISPHERE'), F.col('count').alias('STATION COUNT'))\
             .filter(F.col('HEMISPHERE') == 'Southern Hemisphere').show()
'''
stations_data.filter(F.col('LATITUDE') < 0).count()

''' Output
25336
'''

## How many stations are there in total in the territories of the United States around the world, ##
## excluding the United States itself?
countries_data_with_stations_count.filter(F.col('COUNTRYNAME').contains('[United States]')).agg(F.sum('NUMBEROFSTATIONS')).show()
## OR THE FOLLOWING
#countries_data_with_stations_count.filter(F.col('COUNTRYNAME').contains('[United States]')).agg(F.sum('count')).collect()[0][0]

''' Output
+---------------------+
|sum(NUMBEROFSTATIONS)|
+---------------------+
|                  314|
+---------------------+
'''

# countries_data_with_stations_count.filter(F.col('COUNTRYNAME').contains('[United States]')).show(truncate = False)
''' the territories of United States excluding the United States itself 
+----+----------------------------------------+----------------+
|CODE|COUNTRYNAME                             |NUMBEROFSTATIONS|
+----+----------------------------------------+----------------+
|CQ  |Northern Mariana Islands [United States]|11              |
|WQ  |Wake Island [United States]             |1               |
|AQ  |American Samoa [United States]          |20              |
|LQ  |Palmyra Atoll [United States]           |3               |
|GQ  |Guam [United States]                    |21              |
|JQ  |Johnston Atoll [United States]          |4               |
|VQ  |Virgin Islands [United States]          |43              |
|RQ  |Puerto Rico [United States]             |211             |
+----+----------------------------------------+----------------+

'''