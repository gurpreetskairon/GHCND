#################################################################################################
## Processing Q1a: How is the data structured? Draw a directory tree to represent this in a    ##
##                 sensible way.                                                               ##
#################################################################################################

hdfs dfs -ls -R /data/ghcnd | awk '{print $8}' |
sed -e 's/[^-][^\/]*\//--/g' -e 's/^/ /' -e 's/-/|/'

# Output
 # |---countries
 # |---daily
 # |-----1763.csv.gz
 # |-----1764.csv.gz
 # |-----1765.csv.gz
 # |-----1766.csv.gz
 # ...
 # ...
 # |-----2018.csv.gz
 # |-----2019.csv.gz
 # |-----2020.csv.gz
 # |---inventory
 # |---states
 # |---stations
#



#################################################################################################
## Processing Q1b: How many years are contained in daily, and how does the size of the data    ##
##                 change?                                                                     ##
#################################################################################################

hdfs dfs –count -v /data/ghcnd/daily

# Output
#   DIR_COUNT   FILE_COUNT       CONTENT_SIZE PATHNAME
#           1          258        16639100391 /data/ghcnd/daily

#size of the daily files
hdfs dfs -du -h -v /data/ghcnd/daily

## Output 
# SIZE     DISK_SPACE_CONSUMED_WITH_ALL_REPLICAS  FULL_PATH_NAME
# 3.3 K    26.2 K                                 /data/ghcnd/daily/1763.csv.gz
# 3.2 K    26.0 K                                 /data/ghcnd/daily/1764.csv.gz
# 3.3 K    26.1 K                                 /data/ghcnd/daily/1765.csv.gz
# 3.3 K    26.1 K                                 /data/ghcnd/daily/1766.csv.gz
# ...
# ...
# 198.0 M  1.5 G                                  /data/ghcnd/daily/2015.csv.gz
# 199.9 M  1.6 G                                  /data/ghcnd/daily/2016.csv.gz
# 197.9 M  1.5 G                                  /data/ghcnd/daily/2017.csv.gz
# 197.2 M  1.5 G                                  /data/ghcnd/daily/2018.csv.gz
# 195.1 M  1.5 G                                  /data/ghcnd/daily/2019.csv.gz
# 30.2 M   241.3 M                                /data/ghcnd/daily/2020.csv.gz



#################################################################################################
## Processing Q1c: What is the total size of all of the data?                                  ##
#################################################################################################

hdfs dfs –du –s –v /data/ghcnd

# Output 
# SIZE         DISK_SPACE_CONSUMED_WITH_ALL_REPLICAS  FULL_PATH_NAME
# 16680610588  133444884704                           /data/ghcnd

## OR
hdfs dfs –du –s -h –v /data/ghcnd

# Output 
# SIZE    DISK_SPACE_CONSUMED_WITH_ALL_REPLICAS  FULL_PATH_NAME
# 15.5 G  124.3 G                                /data/ghcnd


# How much of that is daily?                                                                  ##
hdfs dfs –du –s –v /data/ghcnd/daily

# Output 
# SIZE         DISK_SPACE_CONSUMED_WITH_ALL_REPLICAS  FULL_PATH_NAME
# 16639100391  133112803128                           /data/ghcnd/daily

## OR
hdfs dfs –du –s -h –v /data/ghcnd/daily

# Output 
# SIZE    DISK_SPACE_CONSUMED_WITH_ALL_REPLICAS  FULL_PATH_NAME
# 15.5 G  124.0 G                                /data/ghcnd/daily
