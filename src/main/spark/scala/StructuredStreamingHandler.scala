package scala

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, column, desc, window}

import scala.DateSetHandler.Flight

object StructuredStreamingHandler {
  val spark = SparkSession.builder().master("local").getOrCreate()
  spark.conf.set("spark.sql.shuffle.partitions", "5")
  val staticDataFrame = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("src/data/retail-data/by-day/*.csv")

  def main( args:Array[String]): Unit ={
//    val staticDataFrame = spark.read.format("csv")
//      .option("header", "true")
//      .option("inferSchema", "true")
//      .load("src/data/retail-data/by-day/*.csv")
//    staticDataFrame.createOrReplaceTempView("retail_data")
//    //InvoiceNo,StockCode,Description,Quantity,InvoiceDate,UnitPrice,CustomerID,Country
//    val staticSchema = staticDataFrame.schema
//
//    import org.apache.spark.sql.functions.{window, column, desc, col}
//    staticDataFrame
//      .selectExpr(
//        "CustomerId",
//        "(UnitPrice * Quantity) as total_cost",
//        "InvoiceDate")
//      .groupBy(
//        col("CustomerId"), window(col("InvoiceDate"), "1 day"))
//      .sum("total_cost")
//      .show(5)
    streamHandler(staticDataFrame)
  }

  def streamHandler(source: DataFrame) : Unit ={
    source.createOrReplaceTempView("retail_data")
    val staticSchema = source.schema

    val streamingDataFrame = spark.readStream
      .schema(staticSchema)
      .option("maxFilesPerTrigger", 1)
      .format("csv")
      .option("header", "true")
      .load("src/data/test/")
      //.load("src/data/retail-data/by-day/*.csv")

    println(streamingDataFrame.isStreaming)

    //lazy operation
    val purchaseByCustomerPerHour = streamingDataFrame
      .selectExpr(
        "CustomerId",
        "(UnitPrice * Quantity) as total_cost",
        "InvoiceDate")
      .groupBy(
        col("CustomerId"), window(col("InvoiceDate"), "1 day"))
      .sum("total_cost")

    purchaseByCustomerPerHour.writeStream
      .format("console") // memory = store in-memory table
      .queryName("customer_purchases") // the name of the in-memory table
      .outputMode("complete") // complete = all the counts should be in the table
      .start()
    while ( {
      true
    }){}
  }
}

//+----------+--------------------+------------------+
//|CustomerId|              window|   sum(total_cost)|
//+----------+--------------------+------------------+
//|   17235.0|[2010-12-02 08:00...|             341.9|
//  |   16583.0|[2010-12-01 08:00...|233.45000000000002|
//  |   12748.0|[2010-12-01 08:00...|              4.95|
//  |   17809.0|[2010-12-01 08:00...|              34.8|
//  |   16250.0|[2010-12-01 08:00...|            226.14|
//  |   16244.0|[2010-12-02 08:00...|1056.6299999999994|
//  |   13117.0|[2010-12-02 08:00...|202.11999999999998|
//  |   17460.0|[2010-12-01 08:00...|              19.9|
//  |   12868.0|[2010-12-01 08:00...|             203.3|
//  |   16752.0|[2010-12-02 08:00...|             207.5|
//  |   13491.0|[2010-12-02 08:00...|              98.9|
//  |      null|[2010-12-02 08:00...|431.84999999999985|
//  |   16781.0|[2010-12-02 08:00...|311.24999999999994|
//  |   14092.0|[2010-12-02 08:00...|              -5.9|
//  |   14625.0|[2010-12-02 08:00...|            -37.65|
//  |   13705.0|[2010-12-01 08:00...|318.14000000000004|
//  |   13408.0|[2010-12-01 08:00...|1024.6800000000003|
//  |   14594.0|[2010-12-01 08:00...|254.99999999999997|
//  |   13090.0|[2010-12-01 08:00...|             160.6|
//  |   15111.0|[2010-12-02 08:00...|288.49999999999994|
//  +----------+--------------------+------------------+
