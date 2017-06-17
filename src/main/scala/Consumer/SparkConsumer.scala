package Consumer

import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object SparkConsumer extends App {
  val spark = SparkSession
    .builder
    .appName("StreamLocallyExample")
    .config("spark.master", "local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._
  val ds1: DataFrame = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "mytopic")
    .load()
  val data: Dataset[(String, String)] =ds1.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    .as[(String, String)]
  ds1.printSchema()

 val query: StreamingQuery = data.writeStream
    .outputMode("append")
    .format("console")
    .start()
  query.awaitTermination()
 }
