#Imports
import pyspark
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import rand #Used in version with unqiue idenfitier [Proxy of UUID]
import pyspark_cassandra #Used in newer versions of reads, to save directly to dataframe
sc = pyspark.SparkContext() #Dont use if using the shell.
sql = SQLContext(sc)
df_data = (sql.read
          .format("com.databricks.spark.csv")
          .load("gs://ia_static/mobilewalla/raw/aug_2019/part-014*.csv.gz"))
#Paths for datasets that exist on various places :
#/mnt/data/mobilewalla/output/20181201-20190228/ [3 Months worth of data with folders (output,location,raw,asn_exchange)]
df1 = df_data.selectExpr("_c0 as ifa", "_c1 as app_name","_c2 as bundle_id", "_c3 as store_url" , "_c4 as state", "_c5 as city" , "_c6 as zip", "_c7 as country", "_c8 as latitude", "_c9 as longitude", "_c10 as carrier", "_c11 as iab_category" , "_c12 as timestamp", "_c13 as hwv" , "_c14 as device_category" , "_c15 as device_manufacturer" , "_c16 as device_model" , "_c17 as device_name","_c18 as device_vendor","_c19 as device_year_of_release","_c20 as ip_address","_c21 as os","_c22 as mw_carrier","_c23 as platform","_c24 as user_agent")
df1 = df1.select('ifa','app_name','timestamp','state','city','iab_category','device_model','os','bundle_id') #selection as per latest load
#Data Sanitation
df1 = df1.na.drop()
df1 = df1.dropDuplicates()
df1 = df1.filter(df1.bundle_id != 'NIL')
df1 = df1.filter(df1.state != 'NIL')
df1 = df1.filter(df1.city != 'NIL')
df1 = df1.filter(df1.device_model != 'NIL')
df1 = df1.filter(df1.os != 'NIL')
a = df1.count() # Counting from original dataframe to get consistency results
#df1 = df1.withColumn("s_index", rand()) [If using UUID]
df2 = df1.write.format("org.apache.spark.sql.cassandra").options(table= "similarity_table" , keyspace= "tt").save() #writing to scylla
dataFrame = sql.read.format("org.apache.spark.sql.cassandra").options(table="similarity_table", keyspace="tt").load()  #loading from scylla SQLTABLE [v1 outdated/slow]
data = sc.cassandraTable("tt", "similarity_table").select("ifa","city").where("app_name=?", "Tik Tok").toDF() #loading from scylla dataFrame [v2 Faster]
b = dataFrame.count() #Counting from dataFrame collected from Scylla, for consistency results
print(a)
print(b)
