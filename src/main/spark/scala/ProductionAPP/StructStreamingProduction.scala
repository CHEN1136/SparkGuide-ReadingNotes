package scala.ProductionAPP

import org.apache.spark.sql.SparkSession

object StructStreamingProduction {
  val spark = SparkSession.builder().master("local[*]").enableHiveSupport().getOrCreate()

  def main(args: Array[String]): Unit = {
    checkpointHandle()
  }

  def checkpointHandle(): Unit ={
    val static = spark.read.json("src/data/activity-data")
    val streaming = spark
      .readStream
      .schema(static.schema)
      .option("maxFilesPerTrigger", 10)
      .json("src/data/activity-data")
      .groupBy("gt")
      .count()
    val query = streaming
      .writeStream
      .outputMode("complete")
      .option("checkpointLocation", "spark-checkpoints/some/location/")
      .queryName("test_stream")
      .format("memory")
      .start()
    query.awaitTermination()
  }

}
