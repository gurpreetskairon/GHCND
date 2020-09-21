#################################################################################################
## Processing Q3a: Extract the two character country code from each station code in stations   ##
##                 and store the output as a new column using the withColumn command.          ##
#################################################################################################
stations_with_countryid = stations_data.withColumn("CODE", stations_data.ID.substr(1,2))
stations_with_countryid.show(20, False)
stations_with_countryid.cache()

''' Output
+-----------+--------+---------+---------+-----+------------------------+--------+------------+------+----+
|ID         |LATITUDE|LONGITUDE|ELEVATION|STATE|NAME                    |GSN_FLAG|HCN/CRN_FLAG|WMO_ID|CODE|
+-----------+--------+---------+---------+-----+------------------------+--------+------------+------+----+
|ACW00011604|17.1167 |-61.7833 |10.1     |     |ST JOHNS COOLIDGE FLD   |        |            |      |AC  |
|ACW00011647|17.1333 |-61.7833 |19.2     |     |ST JOHNS                |        |            |      |AC  |
|AE000041196|25.333  |55.517   |34.0     |     |SHARJAH INTER. AIRP     |GSN     |            |41196 |AE  |
|AEM00041194|25.255  |55.364   |10.4     |     |DUBAI INTL              |        |            |41194 |AE  |
|AEM00041217|24.433  |54.651   |26.8     |     |ABU DHABI INTL          |        |            |41217 |AE  |
|AEM00041218|24.262  |55.609   |264.9    |     |AL AIN INTL             |        |            |41218 |AE  |
|AF000040930|35.317  |69.017   |3366.0   |     |NORTH-SALANG            |GSN     |            |40930 |AF  |
|AFM00040938|34.21   |62.228   |977.2    |     |HERAT                   |        |            |40938 |AF  |
|AFM00040948|34.566  |69.212   |1791.3   |     |KABUL INTL              |        |            |40948 |AF  |
|AFM00040990|31.5    |65.85    |1010.0   |     |KANDAHAR AIRPORT        |        |            |40990 |AF  |
|AG000060390|36.7167 |3.25     |24.0     |     |ALGER-DAR EL BEIDA      |GSN     |            |60390 |AG  |
|AG000060590|30.5667 |2.8667   |397.0    |     |EL-GOLEA                |GSN     |            |60590 |AG  |
|AG000060611|28.05   |9.6331   |561.0    |     |IN-AMENAS               |GSN     |            |60611 |AG  |
|AG000060680|22.8    |5.4331   |1362.0   |     |TAMANRASSET             |GSN     |            |60680 |AG  |
|AGE00135039|35.7297 |0.65     |50.0     |     |ORAN-HOPITAL MILITAIRE  |        |            |      |AG  |
|AGE00147704|36.97   |7.79     |161.0    |     |ANNABA-CAP DE GARDE     |        |            |      |AG  |
|AGE00147705|36.78   |3.07     |59.0     |     |ALGIERS-VILLE/UNIVERSITE|        |            |      |AG  |
|AGE00147706|36.8    |3.03     |344.0    |     |ALGIERS-BOUZAREAH       |        |            |      |AG  |
|AGE00147707|36.8    |3.04     |38.0     |     |ALGIERS-CAP CAXINE      |        |            |      |AG  |
|AGE00147708|36.72   |4.05     |222.0    |     |TIZI OUZOU              |        |            |60395 |AG  |
+-----------+--------+---------+---------+-----+------------------------+--------+------------+------+----+
only showing top 20 rows
'''

#################################################################################################
## Processing Q3b: LEFT JOIN stations with countries using your output from part (a).          ##
#################################################################################################
stations_countries = stations_with_countryid.join(countries_data, on = 'CODE', how = "left")
stations_countries.show()

''' Output
+----+-----------+--------+---------+---------+-----+--------------------+--------+------------+------+--------------------+
|CODE|         ID|LATITUDE|LONGITUDE|ELEVATION|STATE|                NAME|GSN_FLAG|HCN/CRN_FLAG|WMO_ID|                NAME|
+----+-----------+--------+---------+---------+-----+--------------------+--------+------------+------+--------------------+
|  AC|ACW00011604| 17.1167| -61.7833|     10.1|     |ST JOHNS COOLIDGE...|        |            |      | Antigua and Barbuda|
|  AC|ACW00011647| 17.1333| -61.7833|     19.2|     |            ST JOHNS|        |            |      | Antigua and Barbuda|
|  AE|AE000041196| 25.3330|  55.5170|     34.0|     | SHARJAH INTER. AIRP|     GSN|            | 41196|United Arab Emirates|
|  AE|AEM00041194| 25.2550|  55.3640|     10.4|     |          DUBAI INTL|        |            | 41194|United Arab Emirates|
|  AE|AEM00041217| 24.4330|  54.6510|     26.8|     |      ABU DHABI INTL|        |            | 41217|United Arab Emirates|
|  AE|AEM00041218| 24.2620|  55.6090|    264.9|     |         AL AIN INTL|        |            | 41218|United Arab Emirates|
|  AF|AF000040930| 35.3170|  69.0170|   3366.0|     |        NORTH-SALANG|     GSN|            | 40930|         Afghanistan|
|  AF|AFM00040938| 34.2100|  62.2280|    977.2|     |               HERAT|        |            | 40938|         Afghanistan|
|  AF|AFM00040948| 34.5660|  69.2120|   1791.3|     |          KABUL INTL|        |            | 40948|         Afghanistan|
|  AF|AFM00040990| 31.5000|  65.8500|   1010.0|     |    KANDAHAR AIRPORT|        |            | 40990|         Afghanistan|
|  AG|AG000060390| 36.7167|   3.2500|     24.0|     |  ALGER-DAR EL BEIDA|     GSN|            | 60390|             Algeria|
|  AG|AG000060590| 30.5667|   2.8667|    397.0|     |            EL-GOLEA|     GSN|            | 60590|             Algeria|
|  AG|AG000060611| 28.0500|   9.6331|    561.0|     |           IN-AMENAS|     GSN|            | 60611|             Algeria|
|  AG|AG000060680| 22.8000|   5.4331|   1362.0|     |         TAMANRASSET|     GSN|            | 60680|             Algeria|
|  AG|AGE00135039| 35.7297|   0.6500|     50.0|     |ORAN-HOPITAL MILI...|        |            |      |             Algeria|
|  AG|AGE00147704| 36.9700|   7.7900|    161.0|     | ANNABA-CAP DE GARDE|        |            |      |             Algeria|
|  AG|AGE00147705| 36.7800|   3.0700|     59.0|     |ALGIERS-VILLE/UNI...|        |            |      |             Algeria|
|  AG|AGE00147706| 36.8000|   3.0300|    344.0|     |   ALGIERS-BOUZAREAH|        |            |      |             Algeria|
|  AG|AGE00147707| 36.8000|   3.0400|     38.0|     |  ALGIERS-CAP CAXINE|        |            |      |             Algeria|
|  AG|AGE00147708| 36.7200|   4.0500|    222.0|     |          TIZI OUZOU|        |            | 60395|             Algeria|
+----+-----------+--------+---------+---------+-----+--------------------+--------+------------+------+--------------------+
only showing top 20 rows
'''

'''## OR USING SPARK SQL                                                                          ##
stations_countries_query = """
SELECT stations_n_countryid_tbl.*, countries_tbl.NAME AS COUNTRY_NAME
FROM stations_n_countryid_tbl 
LEFT JOIN countries_tbl ON stations_n_countryid_tbl.CID = countries_tbl.CODE 
ORDER BY stations_n_countryid_tbl.ID
"""
stations_countries = spark.sql(stations_countries_query)
stations_countries.show(20, False)

stations_data.join(countries, stations_with_countryid.CID == countries_data.CODE).show()

'''

#################################################################################################
## Processing Q3c: LEFT JOIN stations and states, allowing for the fact that state codes are   ##
##                 only provided for stations in the US.                                       ##
#################################################################################################
#states = states_data.selectExpr('CODE as STATE', 'NAME')
stations_states = stations_data.join(states_data, stations_data.STATE == states_data.CODE, how="left")
stations_states.sort(F.desc('STATE')).show(truncate = False)

''' output
+-----------+--------+---------+---------+-----+-------------------+--------+------------+------+----+---------------+
|ID         |LATITUDE|LONGITUDE|ELEVATION|STATE|NAME               |GSN_FLAG|HCN/CRN_FLAG|WMO_ID|CODE|NAME           |
+-----------+--------+---------+---------+-----+-------------------+--------+------------+------+----+---------------+
|CA002100310|62.7333 |-138.8333|1100.0   |YT   |CASINO CREEK       |        |            |      |YT  |YUKON TERRITORY|
|CA002100174|60.8667 |-135.3833|686.0    |YT   |BRYN MYRDDIN FARM  |        |            |      |YT  |YUKON TERRITORY|
|CA002100302|62.1833 |-136.4833|1234.0   |YT   |CARMACKS TOWER     |        |            |      |YT  |YUKON TERRITORY|
|CA002100163|60.0000 |-136.7667|836.0    |YT   |BLANCHARD RIVER    |        |            |      |YT  |YUKON TERRITORY|
|CA002100168|63.0500 |-140.9333|1128.0   |YT   |BRANDT PEAK        |        |            |      |YT  |YUKON TERRITORY|
|CA002100182|61.3667 |-139.0500|806.0    |YT   |BURWASH A          |        |            |      |YT  |YUKON TERRITORY|
|CA002100301|62.1167 |-136.2000|543.0    |YT   |CARMACKS CS        |        |            |71039 |YT  |YUKON TERRITORY|
|CA002100115|60.4667 |-134.8333|820.0    |YT   |ANNIE LAKE ROBINSON|        |            |      |YT  |YUKON TERRITORY|
|CA002100161|62.3667 |-140.8667|663.0    |YT   |BEAVER CREEK YTG   |        |            |      |YT  |YUKON TERRITORY|
|CA002100165|64.2333 |-140.3500|1036.0   |YT   |BOUNDARY           |        |            |      |YT  |YUKON TERRITORY|
|CA002100167|61.4667 |-135.7833|716.0    |YT   |BRAEBURN           |        |            |      |YT  |YUKON TERRITORY|
|CA002100179|61.3667 |-139.0333|807.0    |YT   |BURWASH            |        |            |      |YT  |YUKON TERRITORY|
|CA002100181|61.3667 |-139.0333|805.0    |YT   |BURWASH            |        |            |71001 |YT  |YUKON TERRITORY|
|CA002100200|60.1667 |-134.7000|660.0    |YT   |CARCROSS           |        |            |      |YT  |YUKON TERRITORY|
|CA002100300|62.1000 |-136.3000|525.0    |YT   |CARMACKS           |        |            |      |YT  |YUKON TERRITORY|
|CA002100100|61.6500 |-137.4833|966.0    |YT   |AISHIHIK A         |        |            |      |YT  |YUKON TERRITORY|
|CA002100366|64.4667 |-140.7333|576.0    |YT   |CLINTON CREEK      |        |            |      |YT  |YUKON TERRITORY|
|CA002100120|62.3667 |-133.3833|1158.0   |YT   |ANVIL              |        |            |      |YT  |YUKON TERRITORY|
|CA002100160|62.4167 |-140.8667|649.0    |YT   |BEAVER CREEK A     |        |            |      |YT  |YUKON TERRITORY|
|CA002100164|63.9667 |-139.3500|396.0    |YT   |BONANZA CREEK      |        |            |      |YT  |YUKON TERRITORY|
+-----------+--------+---------+---------+-----+-------------------+--------+------------+------+----+---------------+
only showing top 20 rows
'''

#################################################################################################
## Processing Q3d: Based on inventory, what was the first and last year that each station was  ##
##                 active and collected any element at all?                                    ##
#################################################################################################
#inventory_data.groupBy('ID').agg(F.min(F.col('FIRSTYEAR')), (F.max(F.col('LASTYEAR')))).show()
schema = StructType([
    StructField('ID', StringType(), True),
    StructField('FIRSTYEAR', IntegerType(), True),
    StructField('LASTYEAR', IntegerType(), True),
    StructField('ELEMENTSET', ArrayType(StringType()), True)
])


rdd = sc.parallelize(inventory_data.groupBy('ID').agg(F.min(F.col('FIRSTYEAR')),F.max(F.col('LASTYEAR')),F.collect_set('ELEMENT')).collect())
# Create inventory dataframe
inventory  = spark.createDataFrame(rdd, schema)
inventory.show()


''' output
+-----------+---------+--------+--------------------+
|         ID|FIRSTYEAR|LASTYEAR|          ELEMENTSET|
+-----------+---------+--------+--------------------+
|AGE00147719|     1888|    2020|[TMAX, TMIN, PRCP...|
|AGM00060445|     1957|    2020|[TMAX, TMIN, PRCP...|
|AJ000037679|     1959|    1987|              [PRCP]|
|AJ000037831|     1955|    1987|              [PRCP]|
|AJ000037981|     1959|    1987|              [PRCP]|
|AJ000037989|     1936|    2017|[TMAX, TMIN, PRCP...|
|ALE00100939|     1940|    2000|        [TMAX, PRCP]|
|AM000037719|     1912|    1992|[TMAX, TMIN, PRCP...|
|AM000037897|     1936|    2020|[TMAX, TMIN, PRCP...|
|AQC00914873|     1955|    1967|[WT03, TMAX, TMIN...|
|AR000000002|     1981|    2000|              [PRCP]|
|AR000087007|     1956|    2020|[TMAX, TMIN, PRCP...|
|AR000087374|     1956|    2020|[TMAX, TMIN, PRCP...|
|AR000875850|     1908|    2020|[TMAX, TMIN, PRCP...|
|ARM00087022|     1973|    2020|[TMAX, TMIN, PRCP...|
|ARM00087480|     1965|    2020|[TMAX, TMIN, PRCP...|
|ARM00087509|     1973|    2020|[TMAX, TMIN, PRCP...|
|ARM00087532|     1973|    2020|[TMAX, TMIN, PRCP...|
|ARM00087904|     2003|    2020|[TMAX, TMIN, PRCP...|
|ASN00001003|     1909|    1940|              [PRCP]|
+-----------+---------+--------+--------------------+
only showing top 20 rows

'''

'''## OR USING SPARK SQL                                                                          ##
inventory_elements_query = """
SELECT DISTINCT inventory_tbl.ID, inventory_tbl.ELEMENT, inventory_tbl.FIRSTYEAR, inventory_tbl.LASTYEAR
FROM inventory_tbl
WHERE inventory_tbl.ELEMENT is NOT NULL
"""
inventory_elements = spark.sql(inventory_elements_query)
inventory_elements.show()
inventory_elements.count()
'''


##      How many different elements has each station collected overall?                        ##
inventory_data.groupBy('ID').agg(F.countDistinct('ELEMENT').alias('Count_of_Elements')).sort(F.desc('Count_of_Elements')).show()

''' Output
+-----------+-----------------+
|         ID|Count_of_Elements|
+-----------+-----------------+
|USW00014607|               62|
|USW00013880|               61|
|USW00023066|               60|
|USW00013958|               59|
|USW00024121|               58|
|USW00093817|               58|
|USW00093058|               57|
|USW00014944|               57|
|USW00024127|               56|
|USW00024156|               56|
|USW00024157|               56|
|USW00094908|               54|
|USW00014914|               54|
|USW00025309|               54|
|USW00094849|               54|
|USW00026510|               54|
|USW00093822|               54|
|USW00013722|               53|
|USW00003813|               53|
|USW00023065|               52|
+-----------+-----------------+
only showing top 20 rows
'''

'''## OR USING SPARK SQL                                                                          ##
inventory_elementcount_query = """
SELECT inventory_tbl.ID AS STATION_ID, count(inventory_tbl.ELEMENT) AS NUMBER_OF_ELEMENTS
FROM inventory_tbl
GROUP BY inventory_tbl.ID
"""
inventory_elementcount = spark.sql(inventory_elementcount_query)
inventory_elementcount.show()
'''

'''
def filter_Elementset(elementset):
    
    return len(elementset)

udf_filter_Elementset = F.udf(filter_Elementset)
inventory.withColumn('ELEMENTCOUNT', udf_filter_Elementset(inventory.ELEMENTSET)).sort(F.desc('ELEMENTCOUNT')).show(truncate = False)

'''


##      Count the number of core elements that each station has collected overall              ##
countOnConditon = lambda cond: F.sum(F.when(cond, 1).otherwise(0))
'''inventory_data.groupBy('ID').agg(
    countOnConditon((F.col('ELEMENT') == 'TMIN') | (F.col('ELEMENT') == 'TMAX') |\
    (F.col('ELEMENT') == 'PRCP') | (F.col('ELEMENT') == 'SNOW') |\
    (F.col('ELEMENT') == 'SNWD')).alias('Count_of_Core_Elements')
).show()
'''
inventory_data.filter(F.col('ELEMENT').isin(['TMIN', 'TMAX', 'PRCP', 'SNOW', 'SNWD']))\
         .groupBy('ID').agg(F.count('ELEMENT')).show()

''' Output
+-----------+----------------------+
|         ID|Count_of_Core_Elements|
+-----------+----------------------+
|AGE00147719|                     3|
|AGM00060445|                     4|
|AJ000037679|                     1|
|AJ000037831|                     1|
|AJ000037981|                     1|
|AJ000037989|                     4|
|ALE00100939|                     2|
|AM000037719|                     4|
|AM000037897|                     4|
|AQC00914873|                     5|
|AR000000002|                     1|
|AR000087007|                     4|
|AR000087374|                     4|
|AR000875850|                     4|
|ARM00087022|                     3|
|ARM00087480|                     4|
|ARM00087509|                     4|
|ARM00087532|                     4|
|ARM00087904|                     4|
|ASN00001003|                     1|
+-----------+----------------------+
only showing top 20 rows
'''

'''## OR USING SPARK SQL                                                                          ##
inventory_core_elements_query = """
SELECT inventory_tbl.ID AS STATION_ID, count(inventory_tbl.ELEMENT) AS COUNT_OF_CORE_ELEMENTS
FROM inventory_tbl
WHERE inventory_tbl.ELEMENT IN ('TMIN', 'TMAX', 'PRCP','SNOW', 'SNWD')
GROUP BY inventory_tbl.ID
"""
inventory_core_elements = spark.sql(inventory_core_elements_query)
inventory_core_elements.show()
inventory_core_elements.count()
'''

#inventory.groupBy('ID').count().show()

##      Count the number of other (non-core) elements that each station has collected overall  ##
'''inventory_data.groupBy('ID').agg(
    countOnConditon((F.col('ELEMENT') != 'TMIN') & (F.col('ELEMENT') != 'TMAX') &\
    (F.col('ELEMENT') != 'PRCP') & (F.col('ELEMENT') != 'SNOW') &\
    (F.col('ELEMENT') != 'SNWD')).alias('Count_of_Non_Core_Elements')
).show()
'''

inventory_data.filter(~F.col('ELEMENT').isin(['TMIN', 'TMAX', 'PRCP', 'SNOW', 'SNWD']))\
         .groupBy('ID').agg(F.count('ELEMENT')).show()

''' Output
+-----------+--------------------------+
|         ID|Count_of_Non_Core_Elements|
+-----------+--------------------------+
|CA006016970|                         1|
|CA006017400|                         0|
|CA006024010|                         2|
|CA00602K300|                         2|
|CA006032119|                         0|
|CA006037775|                         1|
|CA006037803|                         2|
|CA006040786|                         0|
|CA006040790|                         0|
|CA006045675|                         0|
|CA006046164|                         0|
|CA006046590|                         1|
|CA006050801|                         2|
|CA006051R65|                         0|
|CA006052258|                         2|
|CA006052563|                         2|
|CA006065015|                         0|
|CA006066873|                         2|
|CA006068980|                         2|
|CA006069165|                         2|
+-----------+--------------------------+
only showing top 20 rows
'''

'''## OR USING SPARK SQL                                                                          ##
inventory_not_core_elements_query = """
SELECT inventory_tbl.ID AS STATION_ID, count(inventory_tbl.ELEMENT) AS COUNT_OF_NON_CORE_ELEMENTS
FROM inventory_tbl
WHERE inventory_tbl.ELEMENT NOT IN ('TMIN', 'TMAX', 'PRCP','SNOW', 'SNWD')
GROUP BY inventory_tbl.ID
ORDER BY COUNT_OF_NON_CORE_ELEMENTS DESC
"""
inventory_not_core_elements = spark.sql(inventory_not_core_elements_query)
inventory_not_core_elements.show()
inventory_not_core_elements.count()
'''


##      How many stations collect all five core elements?                                      ##
'''inventory_data.groupBy('ID').agg(
    countOnConditon((F.col('ELEMENT') == 'TMIN') | (F.col('ELEMENT') == 'TMAX') |\
    (F.col('ELEMENT') == 'PRCP') | (F.col('ELEMENT') == 'SNOW') |\
    (F.col('ELEMENT') == 'SNWD')).alias('Count_of_Core_Elements')
).filter(F.col('Count_of_Core_Elements') == 5).count()
'''

inventory_data.filter(F.col('ELEMENT').isin(['TMIN', 'TMAX', 'PRCP', 'SNOW', 'SNWD']))\
         .groupBy('ID').agg(F.count('ELEMENT').alias('CORECOUNT')).filter(F.col('CORECOUNT') == 5).show()

''' Output
20266
'''

'''## OR USING SPARK SQL                                                                          ##
inventory_stations_count_core_elements_query = """
SELECT COUNT(STATION_ID) AS NUMBER_OF_STATIONS_WITH_ALL_5_CORE_ELEMENTS FROM (
    SELECT inventory_tbl.ID AS STATION_ID, count(inventory_tbl.ELEMENT) AS COUNT_OF_CORE_ELEMENTS
    FROM inventory_tbl
    WHERE inventory_tbl.ELEMENT IN ('TMIN', 'TMAX', 'PRCP','SNOW', 'SNWD')
    GROUP BY inventory_tbl.ID) 
WHERE COUNT_OF_CORE_ELEMENTS = 5
"""
inventory_stations_count_core_elements = spark.sql(inventory_stations_count_core_elements_query)
inventory_stations_count_core_elements.show()
'''


##      How many only collected precipitation?                                                 ##
inventory_single_element = inventory_data.groupBy('ID').count().filter(F.col('count') == 1)
inventory_single_element.join(inventory_data, on = 'ID', how= 'left').filter(F.col('ELEMENT') == 'PRCP').sort('ELEMENT').show()
inventory_single_element.count()

''' Output
16123
'''

'''## OR USING SPARK SQL                                                                          ##
inventory_stations_collect_only_precipitation_element_query = """
SELECT COUNT(inventory_tbl.ID) AS NUMBER_OF_STATIONS_COLLECTING_ONLY_PRCP
FROM inventory_tbl 
WHERE inventory_tbl.ID in (
    SELECT ID FROM (
        SELECT inventory_tbl.ID AS ID, count(inventory_tbl.ELEMENT) AS COUNT
        FROM inventory_tbl
        GROUP BY inventory_tbl.ID)
    WHERE COUNT = 1) 
AND inventory_tbl.ELEMENT IN ('PRCP')
"""
inventory_stations_collect_only_precipitation_element = \
                         spark.sql(inventory_stations_collect_only_precipitation_element_query)
inventory_stations_collect_only_precipitation_element.show()
'''


#################################################################################################
## Processing Q3e: LEFT JOIN stations and your output from part (d).                           ##
#################################################################################################
stations_inventory = stations_data.join(inventory, on = 'ID', how = "left")#\
stations_inventory.show()

''' Output
+-----------+--------+---------+---------+-----+--------------------+--------+------------+------+--------+---------+---------+--------+--------------------+
|         ID|LATITUDE|LONGITUDE|ELEVATION|STATE|                NAME|GSN_FLAG|HCN/CRN_FLAG|WMO_ID|LATITUDE|LONGITUDE|FIRSTYEAR|LASTYEAR|          ELEMENTSET|
+-----------+--------+---------+---------+-----+--------------------+--------+------------+------+--------+---------+---------+--------+--------------------+
|AGE00147719| 33.7997|   2.8900|    767.0|     |            LAGHOUAT|        |            | 60545| 33.7997|     2.89|     1888|    2020|[TMAX, TMIN, PRCP...|
|AGM00060445| 36.1780|   5.3240|   1050.0|     |     SETIF AIN ARNAT|        |            | 60445|  36.178|    5.324|     1957|    2020|[TMAX, TMIN, PRCP...|
|AJ000037679| 41.1000|  49.2000|    -26.0|     |             SIASAN'|        |            | 37679|    41.1|     49.2|     1959|    1987|              [PRCP]|
|AJ000037831| 40.4000|  47.0000|    160.0|     |          MIR-BASHIR|        |            | 37831|    40.4|     47.0|     1955|    1987|              [PRCP]|
|AJ000037981| 38.9000|  48.2000|    794.0|     |            JARDIMLY|        |            | 37981|    38.9|     48.2|     1959|    1987|              [PRCP]|
|AJ000037989| 38.5000|  48.9000|    -22.0|     |              ASTARA|     GSN|            | 37989|    38.5|     48.9|     1936|    2017|[TMAX, TMIN, PRCP...|
|ALE00100939| 41.3331|  19.7831|     89.0|     |              TIRANA|        |            |      | 41.3331|  19.7831|     1940|    2000|        [TMAX, PRCP]|
|AM000037719| 40.6000|  45.3500|   1834.0|     |           CHAMBARAK|        |            | 37719|    40.6|    45.35|     1912|    1992|[TMAX, TMIN, PRCP...|
|AM000037897| 39.5330|  46.0170|   1581.0|     |              SISIAN|        |            | 37897|  39.533|   46.017|     1936|    2020|[TMAX, TMIN, PRCP...|
|AQC00914873|-14.3500|-170.7667|     14.9|   AS|    TAPUTIMU TUTUILA|        |            |      |  -14.35|-170.7667|     1955|    1967|[WT03, TMAX, TMIN...|
|AR000000002|-29.8200| -57.4200|     75.0|     |            BONPLAND|        |            |      |  -29.82|   -57.42|     1981|    2000|              [PRCP]|
|AR000087007|-22.1000| -65.6000|   3479.0|     | LA QUIACA OBSERVATO|     GSN|            | 87007|   -22.1|    -65.6|     1956|    2020|[TMAX, TMIN, PRCP...|
|AR000087374|-31.7830| -60.4830|     74.0|     |         PARANA AERO|     GSN|            | 87374| -31.783|  -60.483|     1956|    2020|[TMAX, TMIN, PRCP...|
|AR000875850|-34.5830| -58.4830|     25.0|     | BUENOS AIRES OBSERV|        |            | 87585| -34.583|  -58.483|     1908|    2020|[TMAX, TMIN, PRCP...|
|ARM00087022|-22.6200| -63.7940|    449.0|     |GENERAL ENRIQUE M...|        |            | 87022|  -22.62|  -63.794|     1973|    2020|[TMAX, TMIN, PRCP...|
|ARM00087480|-32.9040| -60.7850|     25.9|     |             ROSARIO|        |            | 87480| -32.904|  -60.785|     1965|    2020|[TMAX, TMIN, PRCP...|
|ARM00087509|-34.5880| -68.4030|    752.9|     |          SAN RAFAEL|        |            | 87509| -34.588|  -68.403|     1973|    2020|[TMAX, TMIN, PRCP...|
|ARM00087532|-35.6960| -63.7580|    139.9|     |        GENERAL PICO|        |            | 87532| -35.696|  -63.758|     1973|    2020|[TMAX, TMIN, PRCP...|
|ARM00087904|-50.2670| -72.0500|    204.0|     |    EL CALAFATE AERO|        |            | 87904| -50.267|   -72.05|     2003|    2020|[TMAX, TMIN, PRCP...|
|ASN00001003|-14.1331| 126.7158|      5.0|     |        PAGO MISSION|        |            |      |-14.1331| 126.7158|     1909|    1940|              [PRCP]|
+-----------+--------+---------+---------+-----+--------------------+--------+------------+------+--------+---------+---------+--------+--------------------+
only showing top 20 rows
'''

'''## OR USING SPARK SQL                                                                          ##
stations_inventory_query = """
SELECT stations_tbl.*, inventory_tbl.ELEMENT, inventory_tbl.FIRSTYEAR, inventory_tbl.LASTYEAR
FROM stations_tbl
LEFT JOIN inventory_tbl ON stations_tbl.ID = inventory_tbl.ID
ORDER BY stations_tbl.ID
"""
stations_inventory = spark.sql(stations_inventory_query)
stations_inventory.show()
stations_inventory.count()
'''


# hdfs dfs -rm -r -f ./ScalableAssignment1
# hdfs dfs -mkdir ./ScalableAssignment1
## Write to file
stations_inventory.write.mode("overwrite").parquet('./ScalableAssignment1/stations_inventory.parquet')

# hdfs dfs -ls ./ScalableAssignment1
''' Output
Found 1 items
drwxr-xr-x   - gsi58 gsi58          0 2020-09-05 19:47 ScalableAssignment1/stations_inventory.parquet
'''

df = spark.read.load('./ScalableAssignment1/stations_inventory.parquet')
df.show()
''' Output
+-----------+--------+---------+---------+-----+--------------------+--------+------------+------+---------+--------+--------------------+
|         ID|LATITUDE|LONGITUDE|ELEVATION|STATE|                NAME|GSN_FLAG|HCN/CRN_FLAG|WMO_ID|FIRSTYEAR|LASTYEAR|          ELEMENTSET|
+-----------+--------+---------+---------+-----+--------------------+--------+------------+------+---------+--------+--------------------+
|AE000041196| 25.3330|  55.5170|     34.0|     | SHARJAH INTER. AIRP|     GSN|            | 41196|     1944|    2020|[TMAX, TMIN, PRCP...|
|AEM00041218| 24.2620|  55.6090|    264.9|     |         AL AIN INTL|        |            | 41218|     1994|    2020|[TMAX, TMIN, PRCP...|
|AGE00147715| 35.4200|   8.1197|    863.0|     |             TEBESSA|        |            |      |     1879|    1938|  [TMAX, TMIN, PRCP]|
|AGM00060402| 36.7120|   5.0700|      6.1|     |             SOUMMAM|        |            | 60402|     1973|    2020|[TMAX, TMIN, PRCP...|
|AGM00060430| 36.3000|   2.2330|    721.0|     |             MILIANA|        |            | 60430|     1957|    2020|[TMAX, TMIN, PRCP...|
|AGM00060461| 35.7000|  -0.6500|     22.0|     |           ORAN-PORT|        |            | 60461|     1995|    2017|[TMAX, TMIN, PRCP...|
|AGM00060514| 35.1670|   2.3170|    801.0|     |       KSAR CHELLALA|        |            | 60514|     1995|    2020|[TMAX, TMIN, PRCP...|
|AGM00060515| 35.3330|   4.2060|    459.0|     |           BOU SAADA|        |            | 60515|     1984|    2020|[TMAX, TMIN, PRCP...|
|AJ000037734| 40.8000|  46.0000|    404.0|     |             SHAMHOR|        |            | 37734|     1936|    1991|              [PRCP]|
|AJ000037740| 40.9830|  47.8670|    682.0|     |              QABALA|        |            | 37740|     1936|    2017|[TMAX, TMIN, PRCP...|
|AJ000037742| 40.9000|  47.3000|    313.0|     |ORDJONIKIDZE,ZERN...|        |            | 37742|     1955|    1987|              [PRCP]|
|AJ000037816| 40.5000|  46.1000|   1655.0|     |           DASHKESAN|        |            | 37816|     1963|    1991|              [PRCP]|
|AM000037618| 41.1170|  44.2830|   1509.0|     |              TASHIR|        |            | 37618|     1928|    1992|[TMAX, TMIN, PRCP...|
|AM000037785| 40.3000|  44.4000|   1092.0|     |             ASTARAK|        |            | 37785|     1957|    1992|              [PRCP]|
|AO000066270|-11.4170|  15.1170|   1304.0|     |   WAKU KUNGU (CELA)|     GSN|            | 66270|     1962|    1982|[TMAX, TMIN, PRCP...|
|AQC00914869|-14.3333|-170.7167|      3.0|   AS|   TAFUNA AP TUTUILA|        |            |      |     1956|    1966|[TMAX, TMIN, PRCP...|
|AR000087078|-24.7000| -60.5830|    130.0|     |         LAS LOMITAS|     GSN|            | 87078|     1956|    2020|[TMAX, TMIN, PRCP...|
|AR000087803|-42.9330| -71.1500|    799.0|     |         ESQUEL AERO|     GSN|            | 87803|     1957|    2020|[TMAX, TMIN, PRCP...|
|ARM00087497|-33.0100| -58.6130|     22.9|     |        GUALEGUAYCHU|     GSN|            | 87497|     1965|    2020|[TMAX, TMIN, PRCP...|
|ARM00087582|-34.5590| -58.4160|      5.5|     |AEROPARQUE JORGE ...|        |            | 87582|     1973|    2020|[TMAX, TMIN, PRCP...|
+-----------+--------+---------+---------+-----+--------------------+--------+------------+------+---------+--------+--------------------+
only showing top 20 rows
'''

#################################################################################################
## Processing Q3f: LEFT JOIN your 1000 rows subset of daily and your output from part (e).     ##
#################################################################################################
daily_stations_inventory = daily.join(stations_inventory, on = 'ID', how = "left")
daily_stations_inventory.show()

''' Output
+-----------+--------+-------+-----+----------------+------------+-----------+----------------+--------+---------+---------+-----+--------------------+--------+------------+------+---------+--------+--------------------+
|         ID|    DATE|ELEMENT|VALUE|MEASUREMENT_FLAG|QUALITY_FLAG|SOURCE_FLAG|OBSERVATION_TIME|LATITUDE|LONGITUDE|ELEVATION|STATE|                NAME|GSN_FLAG|HCN/CRN_FLAG|WMO_ID|FIRSTYEAR|LASTYEAR|          ELEMENTSET|
+-----------+--------+-------+-----+----------------+------------+-----------+----------------+--------+---------+---------+-----+--------------------+--------+------------+------+---------+--------+--------------------+
|ASN00043101|20200101|   PRCP|  0.0|            null|        null|          a|            null|-27.3117| 148.8267|    215.0|     |         WERIBONE TM|        |            |      |     2000|    2020|              [PRCP]|
|ASN00088158|20200101|   PRCP|  0.0|            null|        null|          a|            null|-37.2714| 145.2831|    248.0|     |        STRATH CREEK|        |            |      |     1982|    2020|[DWPR, PRCP, MDPR...|
|ASN00092027|20200101|   TMAX|243.0|            null|        null|          a|            null|-42.5519| 147.8753|     14.0|     |ORFORD (AUBIN COURT)|        |            | 95984|     1951|    2020|[MDTX, TMAX, DWPR...|
|ASN00092027|20200101|   TMIN| 85.0|            null|        null|          a|            null|-42.5519| 147.8753|     14.0|     |ORFORD (AUBIN COURT)|        |            | 95984|     1951|    2020|[MDTX, TMAX, DWPR...|
|ASN00092027|20200101|   PRCP|  0.0|            null|        null|          a|            null|-42.5519| 147.8753|     14.0|     |ORFORD (AUBIN COURT)|        |            | 95984|     1951|    2020|[MDTX, TMAX, DWPR...|
|ASN00092047|20200101|   PRCP|  0.0|            null|        null|          a|            null|-42.2708| 147.6942|    375.0|     |          STONEHOUSE|        |            |      |     1938|    2020|[DWPR, PRCP, MDPR...|
|GME00132682|20200101|   TMAX| 40.0|            null|        null|          E|            null| 50.7456|   9.3467|    300.0|     |        ALSFELD-EIFA|        |            |      |     1978|    2020|[TMAX, TMIN, PRCP...|
|GME00132682|20200101|   TMIN|-56.0|            null|        null|          E|            null| 50.7456|   9.3467|    300.0|     |        ALSFELD-EIFA|        |            |      |     1978|    2020|[TMAX, TMIN, PRCP...|
|GME00132682|20200101|   PRCP|  0.0|            null|        null|          E|            null| 50.7456|   9.3467|    300.0|     |        ALSFELD-EIFA|        |            |      |     1978|    2020|[TMAX, TMIN, PRCP...|
|GME00132682|20200101|   SNWD|  0.0|            null|        null|          E|            null| 50.7456|   9.3467|    300.0|     |        ALSFELD-EIFA|        |            |      |     1978|    2020|[TMAX, TMIN, PRCP...|
|LH000026518|20200101|   TMIN| -5.0|            null|        null|          S|            null| 55.6170|  22.2330|    166.0|     |             LAUKUVA|        |            | 26518|     1959|    2020|[TMAX, TMIN, PRCP...|
|LH000026518|20200101|   TAVG| 25.0|               H|        null|          S|            null| 55.6170|  22.2330|    166.0|     |             LAUKUVA|        |            | 26518|     1959|    2020|[TMAX, TMIN, PRCP...|
|NLE00109310|20200101|   PRCP|  0.0|            null|        null|          E|            null| 51.7667|   4.5831|     -1.0|     |             STRIJEN|        |            |      |     1931|    2020|        [PRCP, SNWD]|
|NLE00109310|20200101|   SNWD|  0.0|            null|        null|          E|            null| 51.7667|   4.5831|     -1.0|     |             STRIJEN|        |            |      |     1931|    2020|        [PRCP, SNWD]|
|SWE00138616|20200101|   PRCP|  0.0|            null|        null|          E|            null| 58.7400|  15.3700|     75.0|     |             TJALLMO|        |            |      |     1964|    2020|        [PRCP, SNWD]|
|SWE00138616|20200101|   SNWD|  0.0|            null|        null|          E|            null| 58.7400|  15.3700|     75.0|     |             TJALLMO|        |            |      |     1964|    2020|        [PRCP, SNWD]|
|US1ALBW0088|20200101|   PRCP|  0.0|            null|        null|          N|            null| 31.0373| -87.7417|     74.7|   AL|  BAY MINETTE 11.0 N|        |            |      |     2019|    2020|        [PRCP, SNOW]|
|US1CANP0003|20200101|   PRCP|  0.0|            null|        null|          N|            null| 38.5764|-122.5780|    106.1|   CA|   CALISTOGA 0.4 SSE|        |            |      |     2009|    2020|[PRCP, WESD, SNWD...|
|US1CANP0003|20200101|   SNOW|  0.0|            null|        null|          N|            null| 38.5764|-122.5780|    106.1|   CA|   CALISTOGA 0.4 SSE|        |            |      |     2009|    2020|[PRCP, WESD, SNWD...|
|US1HIHI0040|20200101|   PRCP|  8.0|            null|        null|          N|            null| 19.4338|-155.2417|   1157.9|   HI|       VOLCANO 4.4 S|        |            |      |     2014|    2020|[PRCP, MDPR, DAPR...|
+-----------+--------+-------+-----+----------------+------------+-----------+----------------+--------+---------+---------+-----+--------------------+--------+------------+------+---------+--------+--------------------+
only showing top 20 rows
'''


## LEFT JOIN your 1000 rows subset of daily and your output from part (e). Are there any
## stations in your subset of daily that are not in stations at all?
# left join the 100 observations of daily 2020 file with the stations_inventory enriched dataset
daily_1000_stations_inventory = daily.join(stations_inventory, on = 'ID', how = 'left')

# get the distinct station codes
distinct_daily_1000 = daily_1000_stations_inventory.select('ID').distinct()

# get the distinct station codes in the stations data as well
distinct_stations = stations_data.select('ID').distinct()

#subtract the distinct daily stations codes from the distinct station codes from the stations dataset
distinct_daily_1000.subtract(distinct_stations).count()

''' Output
0
'''

## Are there any stations in your subset of daily that are not in stations at all?             ##
daily_2020_all = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .schema(daily_schema)
    .load("hdfs:////data/ghcnd/daily/2020.csv.gz")
) 
#daily_stations_inventory = daily_2020_all.join(stations_inventory, on = 'ID', how = 'left')
'''
dd = daily_stations_inventory.select('ID', 'ELEMENT', 'ELEMENTSET')
dd.show(truncate = False)
#from pyspark.sql import SparkSession, functions as F

dd.withColumn(
    'ABC', 
    F.array_exists(
        col("ELEMENTSET"),
        (col: Column) => col === 'PRCP'
)).show()
array_contains(F.col('ELEMENTSET'), 'TMAX') AND array_contains(F.col('ELEMENTSET'),'TMIN')).show()
'''

# left join the entire daily 2020 file with the stations_inventory enriched dataset
daily_2020_all_stations_inventory = daily_2020_all.join(stations_inventory, on = 'ID', how = 'left')

# get the distinct station codes
distinct_daily_2020 = daily_2020_all_stations_inventory.select('ID').distinct()

#subtract the distinct daily stations codes from the distinct station codes from the stations dataset
distinct_daily_2020.subtract(distinct_stations).count()


# Are there any stations in your subset of daily that are not in stations at all?
#daily_stations_inventory.filter(F.col('LATITUDE').isNull()).count()

''' Output
0
'''

# Could you determine if there are any stations in daily that are not in stations without using LEFT JOIN?
daily_2020_all_stations = daily_2020_all.select('ID').distinct()
stations_data_IDs = stations_data.select('ID')
daily_2020_all_stations.subtract(stations_data_IDs).count()

''' Output
 0
'''

