#################################################################################################
## Analysis Q4a : Count the number of rows in daily.                                          ##
#################################################################################################
'''
hadoop = sc._jvm.org.apache.hadoop
fs = hadoop.fs.FileSystem
conf = hadoop.conf.Configuration()
path = hadoop.fs.Path("hdfs:////data/ghcnd/daily/" )
count = 0
for f in fs.get(conf).listStatus(path):
	filename =  str(f.getPath())
	x = filename.split('/')
	filename = "hdfs:////data/ghcnd/daily/" + x[len(x)-1]
	print(filename)
	daily = (
		spark.read.format("com.databricks.spark.csv")
		.option("header", "false")
		.option("inferSchema", "false")
		.schema(daily_schema)
		.load(filename)
	)
	count += daily.count()
print(count)

## OR ALTERNATELY WE COULD USE THE FOLLOWING WAY                                             ##
'''
daily_all = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .schema(daily_schema)
    .load("hdfs:////data/ghcnd/daily/*.csv.gz")
)
daily_all.count()

''' Output
2928664523
'''


#################################################################################################
## Analysis Q4b: How many observations are there for each of the five core elements?           ##
#################################################################################################

observations_with_core_elements = daily_all.filter(F.col('ELEMENT').\
isin(['TMIN', 'TMAX', 'PRCP', 'SNOW', 'SNWD'])).groupBy('ELEMENT').agg(F.count('ID'))
observations_with_core_elements.show()

''' Output
+-------+----------+
|ELEMENT| count(ID)|
+-------+----------+
|   SNOW| 332430532|
|   SNWD| 283572167|
|   PRCP|1021682210|
|   TMIN| 435296249|
|   TMAX| 436709350|
+-------+----------+
'''


#################################################################################################
## Analysis Q4c: Determine how many observations of TMIN do not have a corresponding observation#
##               of TMAX.                                                                      ##
#################################################################################################
'''
# First, get the Stations IDs from inventory, which collect both TMIN and TMAX.
IDs_Collecting_T_elements = inventory_data.groupBy('ID').agg(
    countOnConditon((F.col('ELEMENT') == 'TMIN') | (F.col('ELEMENT') == 'TMAX')).alias('Count_of_T_Elements')
).filter(F.col('Count_of_T_Elements') == 2).selectExpr('ID as TID')

# Now, join the daily_all with the above sations IDs and filterout the ones where the stations 
# IDs are null. This should list all the observations for the required station IDs
x = daily_all.join(IDs_Collecting_T_elements, daily_all.ID == IDs_Collecting_T_elements.TID, \
how = 'left').filter(F.col('TID').isNotNull())
'''

# funtion to say whether or not the station ID collected TMIN
def isOnlyTMIN(elementset):
    if(len(elementset) == 1 and elementset[0] == 'TMIN'):
        return True
    return False

udf_isOnlyTMIN = F.udf(isOnlyTMIN)

# First, filter all the daily observations for TMIN or TMAX values in the ELEMENT field
daily_all_ts = daily_all.filter(F.col('ELEMENT').isin({'TMIN', 'TMAX'}))

# Now, collect all the elements per station ID per day
daily_all_T_elements = daily_all_ts.groupBy('ID', 'DATE').agg(F.collect_set('ELEMENT').alias('ELEMENTSET'))

# find out if an aobservation collected TMIN 
daily_all_Only_TMIN = daily_all_T_elements.withColumn('isOnlyTMIN', udf_isOnlyTMIN('ELEMENTSET'))

# now, filter to get the ones that do collect TMIN, but did not collect TMAX, whcih that stations ID is expected to
daily_all_Only_TMIN.filter(F.col('isOnlyTMIN') == True).count()

''' Output
8428801
'''

## How many different stations contributed to these observations?                               ##
daily_all_Only_TMIN.filter(F.col('isOnlyTMIN') == True).select('ID').distinct().count()

''' Output
27526
'''


#################################################################################################
## Analysis Q4d: Filter daily to obtain all observations of TMIN and TMAX for all stations in  ##
##               New Zealand, and save the result to your output directory.                    ##
#################################################################################################

# add a coutry code and year column to the daily_all dataframe and filyter only those observations
# that have country code as NZ for New Zealand
daily_all_nz = daily_all.withColumn("COUNTRYCODE", daily_all.ID.substr(1,2))\
.withColumn('YEAR', daily_all.DATE.substr(1,4)).filter(F.col('COUNTRYCODE') == 'NZ')

# Now, filter all the elements that collect either TMIN or TMAX
daily_all_nz_T_elements = daily_all_nz.filter(F.col('ELEMENT').isin(['TMIN', 'TMAX']))

# Save the results to an output file of parquet format
daily_all_nz_T_elements.write.mode("overwrite").option('header', True).csv('./ScalableAssignment1/daily_all_nz_T_elements.csv')

# hdfs dfs -ls ./ScalableAssignment1
''' Output
Found 5 items
drwxr-xr-x   - gsi58 gsi58          0 2020-09-06 21:01 ScalableAssignment1/countries_data_with_stations_count.parquet
drwxr-xr-x   - gsi58 gsi58          0 2020-09-08 17:42 ScalableAssignment1/daily_all_nz_T_elements.csv
drwxr-xr-x   - gsi58 gsi58          0 2020-09-08 16:36 ScalableAssignment1/daily_all_nz_T_elements.parquet
drwxr-xr-x   - gsi58 gsi58          0 2020-09-06 21:44 ScalableAssignment1/states_data_with_stations_count.parquet
drwxr-xr-x   - gsi58 gsi58          0 2020-09-08 16:26 ScalableAssignment1/stations_inventory.parquet
'''

# read from the above saved file to see if it got saved properly
df = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", True)
    .load('./ScalableAssignment1/daily_all_nz_T_elements.csv')
)
df.show()

''' Output
+-----------+--------+-------+-----+----------------+------------+-----------+----------------+-----------+----+
|         ID|    DATE|ELEMENT|VALUE|MEASUREMENT_FLAG|QUALITY_FLAG|SOURCE_FLAG|OBSERVATION_TIME|COUNTRYCODE|YEAR|
+-----------+--------+-------+-----+----------------+------------+-----------+----------------+-----------+----+
|NZ000936150|20030101|   TMAX|183.0|            null|        null|          G|            null|         NZ|2003|
|NZ000936150|20030101|   TMIN|124.0|            null|        null|          G|            null|         NZ|2003|
|NZ000093012|20030101|   TMAX|247.0|            null|        null|          G|            null|         NZ|2003|
|NZ000093012|20030101|   TMIN|142.0|            null|        null|          G|            null|         NZ|2003|
|NZ000939870|20030101|   TMAX|163.0|            null|        null|          G|            null|         NZ|2003|
|NZ000939870|20030101|   TMIN|127.0|            null|        null|          G|            null|         NZ|2003|
|NZM00093110|20030101|   TMAX|259.0|            null|        null|          S|            null|         NZ|2003|
|NZM00093110|20030101|   TMIN|127.0|            null|        null|          S|            null|         NZ|2003|
|NZM00093678|20030101|   TMAX|192.0|            null|        null|          S|            null|         NZ|2003|
|NZ000093292|20030101|   TMAX|252.0|            null|        null|          G|            null|         NZ|2003|
|NZ000093292|20030101|   TMIN|105.0|            null|        null|          G|            null|         NZ|2003|
|NZ000093994|20030101|   TMAX|242.0|            null|        null|          G|            null|         NZ|2003|
|NZ000093994|20030101|   TMIN|210.0|            null|        null|          G|            null|         NZ|2003|
|NZ000937470|20030101|   TMAX|319.0|            null|        null|          G|            null|         NZ|2003|
|NZ000937470|20030101|   TMIN| 93.0|            null|        null|          G|            null|         NZ|2003|
|NZ000939450|20030101|   TMAX|111.0|            null|        null|          G|            null|         NZ|2003|
|NZ000939450|20030101|   TMIN| 84.0|            null|        null|          G|            null|         NZ|2003|
|NZM00093781|20030101|   TMAX|218.0|            null|        null|          S|            null|         NZ|2003|
|NZM00093439|20030101|   TMAX|228.0|            null|        null|          S|            null|         NZ|2003|
|NZM00093439|20030101|   TMIN|134.0|            null|        null|          S|            null|         NZ|2003|
+-----------+--------+-------+-----+----------------+------------+-----------+----------------+-----------+----+
only showing top 20 rows

'''


## How many observations are there, 
daily_all_nz_T_elements.count()

''' Output
458892
'''


## and how many years are covered by the observations?
daily_all_nz_T_elements.select("YEAR").distinct().count()

''' Output
81
'''

## copy file daily_all_nz_T_elements.csv file to local
hdfs dfs -copyToLocal ./ScalableAssignment1/daily_all_nz_T_elements.csv ./Scalable/Assignment1/Output

''' Output
drwxr-xr-x. 2 gsi58@canterbury.ac.nz domain users@canterbury.ac.nz 0 Sep  8 18:17 daily_all_nz_T_elements.csv
'''


## copy the output from HDFS to your local home directory, and count the number of rows in the part files

# count the number of rows in the part files using the wc -l bash command.
cat  ./Scalable/Assignment1/Output/daily_all_nz_T_elements.csv/part* | wc -l

''' Output
458973
'''


#################################################################################################
## Analysis Q4e: Group the precipitation observations by year and country. Compute the average ##
##               rainfall in each year for each country, and save this result to your output   ##
##               directory.                                                                    ##
#################################################################################################

# add a coutry code and year column to the daily_all dataframe
daily_all_with_year = daily_all.withColumn("CODE", daily_all.ID.substr(1,2))\
.withColumn('YEAR', daily_all.DATE.substr(1,4))

# join with country table to get the country name
daily_all_with_year_countryname = daily_all_with_year.join(countries_data, on = 'CODE', how = 'left')

# filter the preciptation records and group by year and country
average_rainfall = daily_all_with_year_countryname.filter(F.col('ELEMENT') == 'PRCP')\
                  .groupBy('YEAR', 'CODE', 'NAME').agg(F.avg('VALUE').alias('Average_Rainfall')))

# Save the results to an output file of parquet format
average_rainfall.write.mode("overwrite").option('header', True).csv('./ScalableAssignment1/average_rainfall.csv')

# hdfs dfs -ls ./ScalableAssignment1
''' Output
Found 6 items
drwxr-xr-x   - gsi58 gsi58          0 2020-09-08 18:18 ScalableAssignment1/average_rainfall.csv
drwxr-xr-x   - gsi58 gsi58          0 2020-09-06 21:01 ScalableAssignment1/countries_data_with_stations_count.parquet
drwxr-xr-x   - gsi58 gsi58          0 2020-09-08 17:42 ScalableAssignment1/daily_all_nz_T_elements.csv
drwxr-xr-x   - gsi58 gsi58          0 2020-09-08 16:36 ScalableAssignment1/daily_all_nz_T_elements.parquet
drwxr-xr-x   - gsi58 gsi58          0 2020-09-06 21:44 ScalableAssignment1/states_data_with_stations_count.parquet
drwxr-xr-x   - gsi58 gsi58          0 2020-09-08 16:26 ScalableAssignment1/stations_inventory.parquet
'''

# read from the above saved file to see if it got saved properly
df = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", True)
    .load('./ScalableAssignment1/average_rainfall.csv')
)
df.sortF.desc('Average_rainfall')).show()


''' Output
+----+----+-------+------------------+
|YEAR|CODE|   NAME|  Average_Rainfall|
+----+----+-------+------------------+
|1781|  GM|Germany| 24.55890410958904|
|1782|  GM|Germany|13.712328767123287|
|1783|  GM|Germany|17.832876712328765|
|1784|  GM|Germany| 16.57650273224044|
|1785|  GM|Germany| 16.16986301369863|
|1786|  GM|Germany|24.167123287671235|
|1789|  GM|Germany|18.476712328767125|
|1790|  GM|Germany|15.501369863013698|
|1791|  GM|Germany|  17.4756446991404|
|1792|  GM|Germany|18.117486338797814|
|1794|  GM|Germany|20.753424657534246|
|1795|  GM|Germany|14.487671232876712|
|1796|  GM|Germany| 14.44475138121547|
|1797|  IT|  Italy|12.446575342465753|
|1797|  GM|Germany| 15.95068493150685|
|1798|  IT|  Italy|13.408219178082192|
|1798|  GM|Germany| 16.65753424657534|
|1800|  GM|Germany|16.016438356164382|
|1801|  GM|Germany|19.416438356164385|
|1802|  IT|  Italy|10.424657534246576|
+----+----+-------+------------------+
only showing top 20 rows
'''

## copy file average_rainfall.csv file to local
hdfs dfs -copyToLocal ./ScalableAssignment1/average_rainfall.csv ./Scalable/Assignment1/Output


## Which country has the highest average rainfall in a single year across the entire dataset?
average_rainfall.sort(F.desc('Average_rainfall')).show(truncate = False)

''' Output
+----+----+--------------------+-----------------+
|YEAR|CODE|                NAME| Average_Rainfall|
+----+----+--------------------+-----------------+
|2018|  WF|Wallis and Futuna...|       99.9921875|
|1873|  EI|             Ireland|99.96164383561644|
|2012|  PA|            Paraguay|99.91859991859992|
|1987|  SH|Saint Helena [Uni...|99.91780821917808|
|2012|  CD|                Chad|99.90378006872852|
|2019|  CT|Central African R...|99.89915966386555|
|2007|  PP|    Papua New Guinea|99.85383678440925|
|2011|  PA|            Paraguay|99.83918406072107|
|1981|  CK|Cocos (Keeling) I...|99.83333333333333|
|2005|  CS|          Costa Rica|99.81132075471699|
|1999|  RW|              Rwanda|99.77777777777777|
|1997|  TV|              Tuvalu|99.77566539923954|
|2011|  BL|             Bolivia|99.77001703577513|
|1995|  FM|Federated States ...|99.76856820107314|
|2012|  BL|             Bolivia| 99.6870852475753|
|1996|  JA|               Japan|99.68427014780451|
|2018|  CM|            Cameroon|99.67735042735043|
|2008|  SG|             Senegal| 99.6631299734748|
|2003|  BX|              Brunei|99.66202090592334|
|1949|  CM|            Cameroon|99.62087912087912|
+----+----+--------------------+-----------------+
only showing top 20 rows
'''

## Average rainfall over the years per country. This is needed for the plot
daily_all_with_year = daily_all.withColumn("CODE", daily_all.ID.substr(1,2))\
.withColumn('YEAR', daily_all.DATE.substr(1,4))

# join with country table to get the country name
daily_all_with_year_countryname = daily_all_with_year.join(countries_data, on = 'CODE', how = 'left')

# filter the preciptation records and group by year and country
average_rain_per_country = daily_all_with_year_countryname.filter(F.col('ELEMENT') == 'PRCP')\
                  .groupBy('CODE', 'NAME').agg(F.avg('VALUE').alias('AVERAGE_RAIN_PER_COUNTRY'))
                  
average_rain_per_country.write.mode("overwrite").option('header', True).csv('./ScalableAssignment1/average_rain_per_country.csv')

# hdfs dfs -ls ./ScalableAssignment1
''' Output
Found 8 items
drwxr-xr-x   - gsi58 gsi58          0 2020-09-09 14:23 ScalableAssignment1/NZ_station_pairs_distance.parquet
drwxr-xr-x   - gsi58 gsi58          0 2020-09-10 09:31 ScalableAssignment1/average_rain_per_country.csv
drwxr-xr-x   - gsi58 gsi58          0 2020-09-09 19:51 ScalableAssignment1/average_rainfall.csv
drwxr-xr-x   - gsi58 gsi58          0 2020-09-06 21:01 ScalableAssignment1/countries_data_with_stations_count.parquet
drwxr-xr-x   - gsi58 gsi58          0 2020-09-08 17:42 ScalableAssignment1/daily_all_nz_T_elements.csv
drwxr-xr-x   - gsi58 gsi58          0 2020-09-08 16:36 ScalableAssignment1/daily_all_nz_T_elements.parquet
drwxr-xr-x   - gsi58 gsi58          0 2020-09-06 21:44 ScalableAssignment1/states_data_with_stations_count.parquet
drwxr-xr-x   - gsi58 gsi58          0 2020-09-10 09:14 ScalableAssignment1/stations_inventory.parquet
'''

# read from the above saved file to see if it got saved properly
avg_rain_per_country = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", True)
    .load('./ScalableAssignment1/average_rain_per_country.csv')
)
avg_rain_per_country.sortF.desc('CODE')).show()


''' Output

'''


## copy file average_rainfall.csv file to local
hdfs dfs -copyToLocal ./ScalableAssignment1/average_rain_per_country.csv ./Scalable/Assignment1/Output