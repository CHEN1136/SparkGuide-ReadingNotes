package scala

import org.apache.spark.sql.SparkSession

import scala.MLHandler.spark

object BasicHandler {
  val spark = SparkSession.builder().master("local").getOrCreate()

  def main(args : Array[String]): Unit = {
    import spark.implicits._
    spark.sparkContext.parallelize(Seq(1, 2, 3)).toDF()
    val df = spark.read.format("json")
      .load("src/data/flight-data/json/2015-summary.json")

    autoDefine()

  }
  def autoDefine():Unit ={
    import org.apache.spark.sql.types.{StructField, StructType, StringType, LongType}
    import org.apache.spark.sql.types.Metadata
    val myManualSchema = StructType(Array(
      StructField("DEST_COUNTRY_NAME", StringType, true),
      StructField("ORIGIN_COUNTRY_NAME", StringType, true),
      StructField("count", LongType, nullable = false,
        Metadata.fromJson("{\"hello\":\"world\"}"))
    ))

    val df = spark.read.format("json").schema(myManualSchema)
      .load("src/data/flight-data/json/2015-summary.json").first()
    println(df)
  }

  def rowHandler() ={
    import org.apache.spark.sql.Row
    val myRow = Row("Hello", null, 1, false)
    myRow(0) // type Any
    myRow(0).asInstanceOf[String] // String
    myRow.getString(0) // String
    myRow.getInt(2) // Int
    val df = spark.read.format("json")
      .load("src/data/flight-data/json/2015-summary.json")

  }

}
