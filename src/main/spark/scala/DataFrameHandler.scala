package scala

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{asc, col, desc, expr, lit}
import org.apache.spark.sql.execution.command.CreateFunctionCommand
import java.lang.Thread.sleep
import org.apache.spark.rdd
import org.apache.spark.sql.catalyst.expressions.SortOrder
object DataFrameHandler {
  val spark = SparkSession.builder().master("local").getOrCreate()
  val flightData2015 = spark
    .read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("src/data/flight-data/csv/2015-summary.csv")
  var df: DataFrame = null
  def main(args: Array[String]): Unit = {
//    sqlHandle(flightData2015)
//    dataFrameHandler(flightData2015)
    //compareMax(flightData2015)
//    import org.apache.spark.sql.types._
//    val b = ByteType

    df = loadData()
    //selectHandler()
    //addNewCol()
    //convert()
    //reservedCK()
    //removeCol()
    //castCol()
    //filterRows()
    //distinctFilter()
    //sampleDF()
    //randomSplit()
    //unionDF()
    //sortRows()
    limitRows()
    //partitionRdd()
    //collectRows()
  }

  def loadData():DataFrame ={
    val df = spark.read.format("json")
      .load("src/data/flight-data/json/2015-summary.json")
    //df.createOrReplaceTempView("dfTable")
    import org.apache.spark.sql.Row
    import org.apache.spark.sql.types.{StructField, StructType, StringType, LongType}
    val myManualSchema = new StructType(Array(
      new StructField("some", StringType, true),
      new StructField("col", StringType, true),
      new StructField("names", LongType, false)))
    val myRows = Seq(Row("Hello", null, 1L))
    val myRDD = spark.sparkContext.parallelize(myRows)
    val myDf = spark.createDataFrame(myRDD, myManualSchema)
    myDf.show()
    df
  }

  def selectHandler(): Unit ={
    df.select("DEST_COUNTRY_NAME").show(2)
    df.select("DEST_COUNTRY_NAME", "ORIGIN_COUNTRY_NAME").show(2)

    import org.apache.spark.sql.functions.{expr, col, column}
    import spark.implicits._
//    df.select(
//      df.col("DEST_COUNTRY_NAME"),
//      col("DEST_COUNTRY_NAME"),
//      column("DEST_COUNTRY_NAME"),
//      'DEST_COUNTRY_NAME,
//      $"DEST_COUNTRY_NAME",
//      expr("DEST_COUNTRY_NAME"))
//      .show(2)
    df.select(expr("DEST_COUNTRY_NAME AS destination")).show(2)
    df.select(col("DEST_COUNTRY_NAME"))
      .withColumnRenamed("DEST_COUNTRY_NAME", "destination")
      .show(2)
    df.select(expr("DEST_COUNTRY_NAME").alias("destination"))
      .show(2)
    df.selectExpr("DEST_COUNTRY_NAME as destination").show(2)
  }

  def addNewCol(): Unit ={
    val t = df.selectExpr(
      "*", // include all original columns
      "(DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) as withinCountry")
    t.printSchema()
    t.show(2)
    //add col
    df.withColumn("numberOne", lit(1)).show(2)

    df.withColumn("Destination", expr("DEST_COUNTRY_NAME")).show(2)


    //df.selectExpr("avg(count)", "count(distinct(DEST_COUNTRY_NAME))").show(2)
  }

  def renamedCol(): Unit ={
    df.withColumnRenamed("DEST_COUNTRY_NAME", "dest").columns
  }

  def reservedCK(): Unit ={
    import org.apache.spark.sql.functions.expr
    val dfWithLongColName = df.withColumn(
      "This Long Column-Name",
      expr("ORIGIN_COUNTRY_NAME"))

    dfWithLongColName.selectExpr(
      "`This Long Column-Name`",
      "`This Long Column-Name` as `new col`")
      .show(2)

    dfWithLongColName.select(col("This Long Column-Name"), col("This Long Column-Name")
      .as("new col")).show(2)
  }

  def removeCol(): Unit ={
    df.drop("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME").show(2)
  }

  def convert(): Unit ={
    import org.apache.spark.sql.functions.lit
    df.select(expr("*"), lit(1).as("One")).printSchema()
    df.selectExpr("*", "2 as One").printSchema()

    df.select("DEST_COUNTRY_NAME", "count")
      .distinct()
      .groupBy("DEST_COUNTRY_NAME")
      .avg("count")
      .withColumnRenamed("avg(count)", "destination_avg")
      .toDF()
      .sort(desc("destination_avg"))
      .show(5)
  }

  def sqlHandle(source : DataFrame): Unit ={

    // make DataFrame into a table or view
    source.createOrReplaceTempView("flight_data_2015")

    val sqlWay = spark.sql(
      """
        SELECT DEST_COUNTRY_NAME, count(1) as count
        FROM flight_data_2015
        GROUP BY DEST_COUNTRY_NAME
        ORDER BY count DESC
        """)

    sqlWay.show()
    sqlWay.explain()

    /***
== Physical Plan ==
(3) Sort [count#22L DESC NULLS LAST], true, 0
+- Exchange rangepartitioning(count#22L DESC NULLS LAST, 200), true, [id=#72]
   +- *(2) HashAggregate(keys=[DEST_COUNTRY_NAME#16], functions=[count(1)])
      +- Exchange hashpartitioning(DEST_COUNTRY_NAME#16, 200), true, [id=#68]
         +- *(1) HashAggregate(keys=[DEST_COUNTRY_NAME#16], functions=[partial_count(1)])
            +- FileScan csv [DEST_COUNTRY_NAME#16] Batched: false, DataFilters: [], Format: CSV, Location: InMemoryFileIndex[file:/D:/Project/Spark_Project/src/data/flight-data/csv/2015-summary.csv], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<DEST_COUNTRY_NAME:string>

     */
  }

  def dataFrameHandler(source:DataFrame): Unit ={
    val dataFrameWay = source.groupBy("DEST_COUNTRY_NAME").count()

    //transformations doesn't change the origin data
    val res = dataFrameWay.orderBy(dataFrameWay("count").desc)
    res.show()
    res.explain()

    /**
== Physical Plan ==
(3) Sort [count#43L DESC NULLS LAST], true, 0
+- Exchange rangepartitioning(count#43L DESC NULLS LAST, 200), true, [id=#139]
   +- *(2) HashAggregate(keys=[DEST_COUNTRY_NAME#16], functions=[count(1)])
      +- Exchange hashpartitioning(DEST_COUNTRY_NAME#16, 200), true, [id=#135]
         +- *(1) HashAggregate(keys=[DEST_COUNTRY_NAME#16], functions=[partial_count(1)])
            +- FileScan csv [DEST_COUNTRY_NAME#16] Batched: false, DataFilters: [], Format: CSV, Location: InMemoryFileIndex[file:/D:/Project/Spark_Project/src/data/flight-data/csv/2015-summary.csv], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<DEST_COUNTRY_NAME:string>
     */
  }

  def compareMax(source: DataFrame):Unit = {

    source.createOrReplaceTempView("flight_data_2015")
    val maxSql = spark.sql("""
    SELECT DEST_COUNTRY_NAME, sum(count) as destination_total
    FROM flight_data_2015
    GROUP BY DEST_COUNTRY_NAME
    ORDER BY sum(count) DESC
    LIMIT 5
    """)
    //maxSql.show()

    import org.apache.spark.sql.functions.max
    source
      .groupBy("DEST_COUNTRY_NAME")
      .sum("count")
      .withColumnRenamed("sum(count)", "destination_total")
      .sort(desc("destination_total"))
      .limit(5)
      .explain()

  }

  def castCol(): Unit ={
    df.withColumn("count2", col("count").cast("String")).printSchema()
  }

  def filterRows(): Unit ={
    df.filter(col("count").%(2).equalTo(0)).show(2)
    df.where("count % 2 == 0").filter("ORIGIN_COUNTRY_NAME != 'Ireland'").show(2)

    df.where(col("count") < 2).where(!col("ORIGIN_COUNTRY_NAME").equalTo("Croatia"))
      .show(2)
    df.where(col("count") < 2).where(col("ORIGIN_COUNTRY_NAME") =!= "Croatia")
      .show(2)
  }

  def distinctFilter(): Unit ={
    println(df.select("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME").distinct().count()) //256
    println(df.select("ORIGIN_COUNTRY_NAME").distinct().count()) //125
  }

  def sampleDF(): Unit ={
    val seed = 5
    val withReplacement = false
    val fraction = 0.5
    df.show(7)
    //+-----------------+-------------------+-----+
    //|DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|
    //+-----------------+-------------------+-----+
    //|    United States|            Romania|   15|
    //|    United States|            Croatia|    1|
    //|    United States|            Ireland|  344|
    //|            Egypt|      United States|   15|
    //|    United States|              India|   62|
    //|    United States|          Singapore|    1|
    //|    United States|            Grenada|   62|
    //+-----------------+-------------------+-----+
    df.sample(withReplacement, fraction, seed).show(7)
    println(df.count()) // 256
    println(df.sample(withReplacement, fraction, seed).count()) //138
    //+-----------------+-------------------+-----+
    //|DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|
    //+-----------------+-------------------+-----+
    //|    United States|            Romania|   15|
    //|    United States|            Croatia|    1|
    //|    United States|              India|   62|
    //|    United States|          Singapore|    1|
    //|    United States|            Grenada|   62|
    //|          Senegal|      United States|   40|
    //|          Moldova|      United States|    1|
    //+-----------------+-------------------+-----+
    //seed = 10
    //|DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|
    //+-----------------+-------------------+-----+
    //|    United States|            Romania|   15|
    //|    United States|              India|   62|
    //|    United States|          Singapore|    1|
    //|          Moldova|      United States|    1|
    //|    United States|   Marshall Islands|   39|
    //|           Guyana|      United States|   64|
    //|         Anguilla|      United States|   41|
    //+-----------------+-------------------+-----+
  }

  def randomSplit(){
    val seed = 10

    //Because this method is designed to be randomized, we will also
    //specify a seed (just replace seed with a number of your choosing in the code block). It’s
    //important to note that if you don’t specify a proportion for each DataFrame that adds up to one,
    //they will be normalized
    val dataFrames = df.randomSplit(Array(0.25, 0.75), seed)
    dataFrames(0).show(5)
    dataFrames(1).show(5)
    //+-----------------+-------------------+-----+
    //|DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|
    //+-----------------+-------------------+-----+
    //|          Algeria|      United States|    4|
    //|        Argentina|      United States|  180|
    //|          Belgium|      United States|  259|
    //|           Canada|      United States| 8399|
    //|   Cayman Islands|      United States|  314|
    //+-----------------+-------------------+-----+
    //+-------------------+-------------------+-----+
    //|  DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|
    //+-------------------+-------------------+-----+
    //|             Angola|      United States|   15|
    //|           Anguilla|      United States|   41|
    //|Antigua and Barbuda|      United States|  126|
    //|              Aruba|      United States|  346|
    //|          Australia|      United States|  329|
    //+-------------------+-------------------+-----+
   }

  def unionDF(): Unit ={
    import org.apache.spark.sql.Row
    import spark.implicits._

    val schema = df.schema
    val newRows = Seq(
      Row("New Country", "Other Country", 5L),
      Row("New Country 2", "Other Country 3", 1L)
    )
    val parallelizedRows = spark.sparkContext.parallelize(newRows)
    val newDF = spark.createDataFrame(parallelizedRows, schema)
    df.union(newDF)
      .where("count = 1")
      .where($"ORIGIN_COUNTRY_NAME" =!= "United States")
      .show()
  }

  def sortRows(): Unit ={
    df.sort("count").show(5)
    df.orderBy("count", "DEST_COUNTRY_NAME").show(5)
    df.orderBy(asc("ORIGIN_COUNTRY_NAME"), desc("count")).show(5)
    df.orderBy(expr("count desc")).show(2)

    //optimization
    spark.read.format("json").load("src/data/flight-data/json/*-summary.json")
      .sortWithinPartitions("count")
    df.orderBy(expr("DEST_COUNTRY_NAME asc_nulls_first")).show(5)
  }

  def limitRows(): Unit ={
    df.limit(5).show()
    df.orderBy(expr("count desc")).limit(6).show() //失效
    //+--------------------+-------------------+-----+
    //|   DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|
    //+--------------------+-------------------+-----+
    //|               Malta|      United States|    1|
    //|Saint Vincent and...|      United States|    1|
    //|       United States|            Croatia|    1|
    //|       United States|          Gibraltar|    1|
    //|       United States|          Singapore|    1|
    //|             Moldova|      United States|    1|
    //+--------------------+-------------------+-----+
    df.orderBy(desc("count")).limit(6).show()
    df.orderBy(expr("count").desc).limit(6).show()
    println(expr("count DESC").getClass) //class org.apache.spark.sql.catalyst.expressions.Alias
    println(expr("count").desc.expr.getClass) //class org.apache.spark.sql.catalyst.expressions.SortOrder
    //+-----------------+-------------------+------+
    //|DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME| count|
    //+-----------------+-------------------+------+
    //|    United States|      United States|370002|
    //|    United States|             Canada|  8483|
    //|           Canada|      United States|  8399|
    //|    United States|             Mexico|  7187|
    //|           Mexico|      United States|  7140|
    //|   United Kingdom|      United States|  2025|
    //+-----------------+-------------------+------+
  }

  def partitionRdd(): Unit ={
    val dfPartByNum = df.repartition(5)
    println(dfPartByNum.rdd.getNumPartitions) //5

    val dfPartByCol = df.repartition(col("DEST_COUNTRY_NAME"))
    println(dfPartByCol.rdd.getNumPartitions) //200

    val dfPartByMixed = df.repartition(5, col("DEST_COUNTRY_NAME"))
    println(dfPartByMixed.rdd.getNumPartitions) //5

    val dfPartByCoalesce = df.repartition(5, col("DEST_COUNTRY_NAME")).coalesce(2)
    println(dfPartByCoalesce.rdd.getNumPartitions) //2
  }

  def collectRows(): Unit ={
    val collectDF = df.limit(10)
    collectDF.take(5) // take works with an Integer count
    collectDF.show() // this prints it out nicely
    collectDF.show(5, false)
    //collectDF.collect()
    //collectDF.toLocalIterator()
  }
}
