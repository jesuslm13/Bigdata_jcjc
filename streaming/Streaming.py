from pyspark.context import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark.streaming.flume import FlumeUtils
import datetime
from pyspark.sql.types import StructField, TimestampType, StringType, StructType
import subprocess

spark_path = "spark://localhost:7077"
hdfs_path = "hdfs://192.168.56.104:9000/jcjc"

def StreamingMr(rdd):
    
    if rdd.isEmpty():
        print("========================rdd is Empty========================")
        
    else:
        
        spark = SparkSession.builder.appName("spark_session").getOrCreate()
        
        # df_temp
        mylog_df = rdd.map(lambda line : line.split("|")).map(lambda line : [datetime.datetime.strptime(line[0].replace('"', ''), "%Y-%m-%d %H:%M:%S"), line[1], line[2], line[3]])
        fields = [StructField("access_time", TimestampType(), True), StructField("user_id", StringType(), True), StructField("session_id", StringType(), True), StructField("pre_post_politician_name", StringType(), True)]
        schema = StructType(fields)
        df = spark.createDataFrame(mylog_df, schema)
        
        rdd = df.rdd
        
        mr = rdd.map(lambda line : (line[3], 1)).reduceByKey(lambda a, b : a+b)
        df_temp = spark.createDataFrame(mr)
        print("df_temp : ============================", type(df_temp))
        print("temp_show : ==========================", df_temp.show())
        
        df_temp = df_temp.withColumnRenamed("_1", "name")
        df_temp = df_temp.withColumnRenamed("_2", "temp_count")
        print("temp_show : ==========================", df_temp.show())
        
        # df_raw
        df_raw=spark.read.csv(hdfs_path+"/log_csv_res/*.csv",header=False)
        print("df_raw : ============================", type(df_raw))
        
        df_raw = df_raw.withColumnRenamed("_c0", "name")
        df_raw = df_raw.withColumnRenamed("_c1", "raw_count")
        
        # merge
        df_mig = df_raw.join(df_temp, df_raw.name == df_temp.name, "left_outer").drop(df_temp.name)
        df_mig = df_mig.fillna(0)
        print("df_mig_join : =======================", df_mig.show())
        df_mig = df_mig.withColumn("count", df_mig.raw_count+df_mig.temp_count)
        print("df_mig : ============================", df_mig.show())
        df_mig = df_mig.select("name", "count")
        print("df_mig : ============================", df_mig.select("*").orderBy("count", ascending=False).show(5))
        
        print("유승민 _df_mig : ============================", df_mig.select("*").where('name like "유승민(劉承旼)/%"').orderBy("count", ascending=False).show(5))
        
        df_mig.coalesce(1).write.csv(path=hdfs_path+"/log_csv_res_temp", mode="overwrite")
        
        subprocess_open("$HADOOP_HOME/bin/hadoop fs -rm -r "+hdfs_path+"/log_csv_res/*")
        subprocess_open("$HADOOP_HOME/bin/hadoop fs -mv "+hdfs_path+"/log_csv_res_temp/* "+hdfs_path+"/log_csv_res/")

def subprocess_open(command):
    popen = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    (stdoutdata, stderrdata) = popen.communicate()
    
    return stdoutdata, stderrdata

if __name__ == '__main__':
    sc = SparkContext(spark_path,"streaming")
    ssc = StreamingContext(sc, 10)
    ds = ssc.textFileStream(hdfs_path+"/access_politician")
#     ds = FlumeUtils.createStream(ssc, "192.168.56.104", 9999)
    ds.pprint()
    ds.foreachRDD(StreamingMr)
   
    ssc.start()
    ssc.awaitTermination()
