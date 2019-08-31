
#-*- coding:UTF-8 -*-

import datetime

from pyspark import SparkContext
from pyspark.python.pyspark.shell import sqlContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as q
from pyspark.sql.functions import udf
from pyspark.sql.types import *
from pyspark.streaming import StreamingContext


# 하둡 path
path = "hdfs://192.168.56.104:9000/jcjc"
# 컨텍스트 생성
sc = SparkContext.getOrCreate();
print(sc)
 
# HDFS 파일 RDD로 읽어오기
mylog = sc.textFile(path+"/access_politician/FlumeData.1561361979518")
print(mylog)
   
# df 변환 준비
mylog_df = mylog.map(lambda line : line.split("|")).map(lambda line : [datetime.datetime.strptime(line[0].replace('"', ''), "%Y-%m-%d %H:%M:%S"), line[1], line[2], line[3]])
print(mylog_df)
fields = [StructField("access_time", TimestampType(), True), StructField("user_id", StringType(), True), StructField("session_id", StringType(), True), StructField("pre_post_politician_name", StringType(), True)]
schema = StructType(fields)
df = sqlContext.createDataFrame(mylog_df, schema)
print(df.show())
 
# csv 저장
df.coalesce(1).write.csv(path+"/log_csv_temp", mode="overwrite")
print("완료")

# MR
    
df = sqlContext.read.csv(path= path+"/log_csv_temp/*.csv", sep=",")
rdd = df.rdd
counts = rdd.map(lambda line : (line[3], 1)).reduceByKey(lambda a, b : a+b)
print(counts.collect(), type(counts))

# df로 변경
res_df = counts.toDF()
  
# # 컬럼 명 지정
res_df = res_df.withColumnRenamed("_1", "pol_log")
res_df = res_df.withColumnRenamed("_2", "count")
 
# csv로 저장
res_df.coalesce(1).write.csv(path+"/log_csv_res_temp", mode="overwrite")

print("완료")

# csv 불러오기

df = sqlContext.read.csv(path= path+"/log_csv_res_temp/*.csv", sep=",")

# 컬럼명 지정
df= df.withColumnRenamed("_c0","pol_log").withColumnRenamed("_c1","count")

# sort
df.select("*").orderBy("count", ascending=False).show(5)