#################################################################################################
## Analysis Q3a : How many blocks are required for the daily climate summaries for the year 2020#
#################################################################################################

hdfs getconf -confKey 'dfs.blocksize'

''' Output
134217728
'''

hdfs fsck /data/ghcnd/daily/2020.csv.gz -files -blocks



#################################################################################################
## Analysis Q3b : Load and count the number of observations in daily for each of the years 2015##
##                and 2020                                                                     ##
#################################################################################################

# Load and count the 2015 data
daily_2015_all = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .schema(daily_schema)
    .load("hdfs:////data/ghcnd/daily/2015.csv.gz")
)
daily_2015_all.count()

''' Output
34899014
'''

# Load and count the 2020 data
daily_2020_all = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "false")
    .option("inferSchema", "false") 
    .schema(daily_schema)
    .load("hdfs:////data/ghcnd/daily/2020.csv.gz")
)
daily_2020_all.count()

''' Output
5215365
'''

## How many tasks were executed by each stage of each job?##


#################################################################################################
## Analysis Q3c : Load and count the number of observations in daily between 2015 to 2020      ##
#################################################################################################

daily_2015_2020 = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .schema(daily_schema)
    .load("hdfs:////data/ghcnd/daily/20{15,16,17,18,19,20}.csv.gz")
)
daily_2015_2020.count()

''' Output
178918901
'''

