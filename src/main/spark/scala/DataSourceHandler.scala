package scala

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

import java.sql.DriverManager
import scala.DataFrameHandler.spark

object DataSourceHandler {
  val spark = SparkSession.builder().master("local[*]").enableHiveSupport().getOrCreate()
  var myManualSchema = new StructType(Array(
    new StructField("DEST_COUNTRY_NAME", StringType, true),
    new StructField("ORIGIN_COUNTRY_NAME", StringType, true),
    new StructField("count", LongType, false)
  ))
  var csvFile: DataFrame = spark.read.format("csv")
    .option("header", "true").option("mode", "FAILFAST").schema(myManualSchema)
    .load("src/data/flight-data/csv/2010-summary.csv")



  def main(args : Array[String]): Unit ={
    //csvReader()
    //csvWriter()
    //jsonReader()
    //jsonWriter()
    //parquetReader()
    //parquetWriter()
    //orcReader()
    //orcWriter()
    //sqlReader()
    //sqlWriter()
    //textFileReader()
    //textFileWriter()
    //parallelReader()
    //parallelWriter()
    bucketWriter()
  }

  def csvReader(): Unit ={
    spark.read.format("csv")
      .option("header", "true")
      .option("mode", "FAILFAST")
      .schema(myManualSchema)
      .load("src/data/flight-data/csv/2010-summary.csv")
      .show(5)
    spark.read
      .option("header", "true")
      .option("mode", "FAILFAST")
      .schema(myManualSchema)
      .csv("src/data/flight-data/csv/2010-summary.csv")
      .show(5)
    //+-----------------+-------------------+-----+
    //|DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|
    //+-----------------+-------------------+-----+
    //|    United States|            Romania|    1|
    //|    United States|            Ireland|  264|
    //|    United States|              India|   69|
    //|            Egypt|      United States|   24|
    //|Equatorial Guinea|      United States|    1|
    //+-----------------+-------------------+-----+
  }

  def csvWriter(): Unit ={
    //For instance, we can take our CSV file and write it out as a TSV file quite easily:
    csvFile.write.format("csv").mode("overwrite").option("sep", "\t")
      .save("tmp/my-tsv-file.tsv")
  }

  def jsonReader(): Unit ={
    spark.read.format("json").option("mode", "FAILFAST").schema(myManualSchema)
      .load("src/data/flight-data/json/2010-summary.json").show(5)
    //+-----------------+-------------------+-----+
    //|DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|
    //+-----------------+-------------------+-----+
    //|    United States|            Romania|    1|
    //|    United States|            Ireland|  264|
    //|    United States|              India|   69|
    //|            Egypt|      United States|   24|
    //|Equatorial Guinea|      United States|    1|
    //+-----------------+-------------------+-----+
  }

  def jsonWriter(): Unit ={
    csvFile.write.format("json").mode("overwrite").save("tmp/my-json-file.json")
  }

  def parquetReader(): Unit ={
    spark.read.format("parquet")
      .load("src/data/flight-data/parquet/2010-summary.parquet").show(5)
    //+-----------------+-------------------+-----+
    //|DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|
    //+-----------------+-------------------+-----+
    //|    United States|            Romania|    1|
    //|    United States|            Ireland|  264|
    //|    United States|              India|   69|
    //|            Egypt|      United States|   24|
    //|Equatorial Guinea|      United States|    1|
    //+-----------------+-------------------+-----+
  }

  def parquetWriter(): Unit ={
    csvFile.write.format("parquet").mode("overwrite")
      .save("tmp/my-parquet-file.parquet")
    /**
      22/05/04 10:43:25 INFO ParquetWriteSupport: Initialized Parquet WriteSupport with Catalyst schema:
      {
        "type" : "struct",
        "fields" : [ {
          "name" : "DEST_COUNTRY_NAME",
          "type" : "string",
          "nullable" : true,
          "metadata" : { }
        }, {
          "name" : "ORIGIN_COUNTRY_NAME",
          "type" : "string",
          "nullable" : true,
          "metadata" : { }
        }, {
          "name" : "count",
          "type" : "long",
          "nullable" : true,
          "metadata" : { }
        } ]
      }
      and corresponding Parquet message type:
      message spark_schema {
        optional binary DEST_COUNTRY_NAME (UTF8);
        optional binary ORIGIN_COUNTRY_NAME (UTF8);
        optional int64 count;
      }
     */
  }

  def orcReader(): Unit ={
    spark.read.format("orc").load("src/data/flight-data/orc/2010-summary.orc").show(5)
    //+-----------------+-------------------+-----+
    //|DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|
    //+-----------------+-------------------+-----+
    //|    United States|            Romania|    1|
    //|    United States|            Ireland|  264|
    //|    United States|              India|   69|
    //|            Egypt|      United States|   24|
    //|Equatorial Guinea|      United States|    1|
    //+-----------------+-------------------+-----+
  }

  def orcWriter(): Unit ={
    csvFile.write.format("orc").mode("overwrite").save("tmp/my-json-file.orc")
  }

  def sqlReader(): Unit ={
    val url = "jdbc:mysql://127.0.0.1:3306/spark_db?characterEncoding=utf8&serverTimezone=Asia/Shanghai&useSSL=false"
    val tableName = "flight_data"
    val props = new java.util.Properties()
    val driver = "com.mysql.cj.jdbc.Driver"
    props.put("user","root")
    props.put("password","ROOTroot_1")
    props.put("driver","com.mysql.cj.jdbc.Driver")
    val connection = DriverManager.getConnection(url, props)
    val dbDataFrame = spark.read.jdbc(url, tableName, props)
    dbDataFrame.show(5)

    spark.read.format("jdbc").option("url", url)
      .option("dbtable", tableName)
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("user", "root")
      .option("password", "ROOTroot_1")
      .load().show(5)
    ////+-----------------+-------------------+-----+
    //    //|DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|
    //    //+-----------------+-------------------+-----+
    //    //|    United States|            Romania|    1|
    //    //|    United States|            Ireland|  264|
    //    //|    United States|              India|   69|
    //    //|            Egypt|      United States|   24|
    //    //|Equatorial Guinea|      United States|    1|
    //    //+-----------------+-------------------+-----+
    dbDataFrame.select("DEST_COUNTRY_NAME").distinct().explain
    /**
 == Physical Plan ==
(2) HashAggregate(keys=[DEST_COUNTRY_NAME#6], functions=[])
+- Exchange hashpartitioning(DEST_COUNTRY_NAME#6, 200), true, [id=#33]
   +- *(1) HashAggregate(keys=[DEST_COUNTRY_NAME#6], functions=[])
      +- *(1) Scan JDBCRelation(flight_data) [numPartitions=1] [DEST_COUNTRY_NAME#6] PushedFilters: [], ReadSchema: struct<DEST_COUNTRY_NAME:string>
     */
    dbDataFrame.filter("DEST_COUNTRY_NAME in ('Anguilla', 'Sweden')").explain
    /*
    == Physical Plan ==
*(1) Scan JDBCRelation(flight_data)  [DEST_COUNTRY_NAME#6,ORIGIN_COUNTRY_NAME#7,count#8L] PushedFilters: [*In(DEST_COUNTRY_NAME, [Anguilla,Sweden])], ReadSchema: struct<DEST_COUNTRY_NAME:string,ORIGIN_COUNTRY_NAME:string,count:bigint>
     */
    val pushdownQuery = """(SELECT DISTINCT(DEST_COUNTRY_NAME) FROM flight_data) as flight_data"""
    spark.read.jdbc(url, pushdownQuery, props).show(5)
    //== Physical Plan ==
    //*(1) Scan JDBCRelation((SELECT DISTINCT(DEST_COUNTRY_NAME) FROM flight_data) as flight_data) [numPartitions=1] [DEST_COUNTRY_NAME#45] PushedFilters: [], ReadSchema: struct<DEST_COUNTRY_NAME:string>

    spark.read.option("numPartitions", 10).jdbc(url, tableName, props).distinct().explain()

    var predicates = Array(
      "DEST_COUNTRY_NAME = 'Sweden' OR ORIGIN_COUNTRY_NAME = 'Sweden'",
      "DEST_COUNTRY_NAME = 'Anguilla' OR ORIGIN_COUNTRY_NAME = 'Anguilla'")
    spark.read.jdbc(url, tableName, predicates, props).show()
    //+-----------------+-------------------+-----+
    //|DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|
    //+-----------------+-------------------+-----+
    //|           Sweden|      United States|   65|
    //|    United States|             Sweden|   73|
    //|         Anguilla|      United States|   21|
    //|    United States|           Anguilla|   20|
    //+-----------------+-------------------+-----+
    println(spark.read.jdbc(url, tableName, predicates, props).rdd.getNumPartitions) //2

    predicates = Array(
      "DEST_COUNTRY_NAME != 'Sweden' OR ORIGIN_COUNTRY_NAME != 'Sweden'",
      "DEST_COUNTRY_NAME != 'Anguilla' OR ORIGIN_COUNTRY_NAME != 'Anguilla'")
    println(spark.read.jdbc(url, tableName, predicates, props).count()) //510

    //slide windows
    val colName = "count"
    val lowerBound = 0L
    val upperBound = 348113L // this is the max count in our database
    val numPartitions = 10
    println(spark.read.jdbc(url, tableName, colName, lowerBound, upperBound, numPartitions, props)
      .count()) //255
    connection.close()
  }

  def sqlWriter(): Unit ={
    val url = "jdbc:mysql://127.0.0.1:3306/spark_db?characterEncoding=utf8&serverTimezone=Asia/Shanghai&useSSL=false"
    val tableName = "flight_data_copy"
    val props = new java.util.Properties()
    props.put("user","root")
    props.put("password","ROOTroot_1")
    props.put("driver","com.mysql.cj.jdbc.Driver")
    csvFile.write.mode("overwrite").jdbc(url, tableName, props) //255
    csvFile.write.mode("append").jdbc(url, tableName, props) //510, 去重问题
    csvFile.write.mode("overwrite").jdbc(url, tableName, props) //255
  }

  def textFileReader(): Unit ={
    import spark.implicits._
    spark.read.textFile("src/data/flight-data/csv/2010-summary.csv")
      .selectExpr("split(value, ',') as rows").show(2,false)
    //+-----------------------------------------------+
    //|rows                                           |
    //+-----------------------------------------------+
    //|[DEST_COUNTRY_NAME, ORIGIN_COUNTRY_NAME, count]|
    //|[United States, Romania, 1]                    |
    //+-----------------------------------------------+
  }

  def textFileWriter(): Unit ={
    csvFile.select("DEST_COUNTRY_NAME").write.mode("overwrite").text("tmp/simple-text-file.txt")
    csvFile.limit(10).select("DEST_COUNTRY_NAME", "count")
      .write.partitionBy("count").text("tmp/five-csv-files2.csv")
  }

  def parallelReader(): Unit ={
    println(spark.read.format("csv")
      .option("header", "true")
      .option("mode", "FAILFAST")
      .schema(myManualSchema)
      .load("src/data/flight-data/csv/*.csv")
      .rdd.getNumPartitions) //6
  }

  def parallelWriter(): Unit ={
    //csvFile.repartition(5).write.format("csv").save("tmp/multiple.csv")
    csvFile.limit(10).write.mode("overwrite").partitionBy("DEST_COUNTRY_NAME")
      .save("tmp/partitioned-files.parquet")
  }

  def bucketWriter(): Unit = {
    val numberBuckets = 10
    val columnToBucketBy = "count"
    csvFile.write.format("parquet").mode("overwrite")
      .bucketBy(numberBuckets, columnToBucketBy).saveAsTable("bucketedFiles")
  }
}
