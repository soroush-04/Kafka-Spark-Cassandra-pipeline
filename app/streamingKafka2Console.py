from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,LongType,IntegerType,FloatType,StringType
from pyspark.sql.functions import split,from_json,col,lit
import torch
import base64
import json

potholeSchema = StructType([
                StructField("id",StringType(),False),
                StructField("name",StringType(),False),
                StructField("content",StringType(),False),
            ])

spark = SparkSession \
    .builder \
    .appName("SSKafka") \
    .config("spark.cassandra.connection.host","cassandra")\
    .config("spark.cassandra.connection.port","9042")\
    .config("spark.cassandra.auth.username","cassandra")\
    .config("spark.cassandra.auth.password","cassandra")\
    .config("spark.driver.host", "localhost")\
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "172.18.0.4:9092") \
  .option("subscribe", "pothole") \
  .option("startingOffsets", "earliest") \
  .load()

df.printSchema()

df1 = df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"),potholeSchema).alias("data")).select("data.*")
df1.printSchema()


def getrows(df, rownums=None):
    return df.rdd.zipWithIndex().filter(lambda x: x[1] in rownums).map(lambda x: x[0])

def foreach_batch_function(df, epoch_id):
    img = getrows(df, rownums=[0]).collect()
    print("got: %s"%img[0][1])
    file = open("./images/%s"%img[0][1], 'wb')
    data = img[0][2] if img[0][2] else b'0'
    file.write(base64.decodestring(eval(data)))
    file.close()
    model = torch.hub.load('ultralytics/yolov5', 'custom', path='best.pt')
    img = "./images/%s"%img[0][1]
    results = model(img)
    results.save()
    # save to db
    # add detection result
    # cord_thres contains 4 corners of bounding box, 5th array parameter is confidence score
    cord_thres = results.xyxyn[0][:, :-1].numpy()
    df.withColumn("cord_thres",lit(json.dumps(cord_thres.tolist())))
    df.write.format("org.apache.spark.sql.cassandra").mode('append').options(table="results", keyspace="ph").save()

df1.writeStream.foreachBatch(foreach_batch_function).start() \
  .awaitTermination()
