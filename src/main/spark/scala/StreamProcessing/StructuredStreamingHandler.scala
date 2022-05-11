package scala.StreamProcessing

import org.apache.spark.sql.functions.{col, window}
import org.apache.spark.sql.{DataFrame, SparkSession}

object StructuredStreamingHandler {
  val spark = SparkSession.builder().master("local").getOrCreate()
  spark.conf.set("spark.sql.shuffle.partitions", "5")
  val staticDataFrame = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("src/data/test/*.csv")

  def main(args: Array[String]): Unit = {
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

  def streamHandler(source: DataFrame): Unit = {
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
    val stream = purchaseByCustomerPerHour.writeStream
      .format("console") // memory = store in-memory table
      .queryName("customer_purchases") // the name of the in-memory table
      .outputMode("complete") // complete = all the counts should be in the table
      .start()
//    purchaseByCustomerPerHour.show(2)
    stream.awaitTermination()
  }
}
