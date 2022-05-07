package scala

import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.functions.{approx_count_distinct, avg, col, count, countDistinct, desc, explode, expr, sum, sumDistinct, to_date, udaf}
import org.apache.spark.sql.types.{BooleanType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, Row, SparkSession, functions}

import scala.DataFrameHandler.spark
import scala.DataTypeHandler.spark
import scala.UDF.{BoolAnd, Invoice, MyAverage}

object AggregationHandler {
  val spark = SparkSession.builder().master("local").getOrCreate()
  spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

  var df: DataFrame = null
  def main(args: Array[String]): Unit = {
    df = loadData()
    //countHandle()
    //first_and_last()
    //min_and_max()
    //sumHandle()
    //avgHandle()
    //variance_and_standard_deviation()
    //skewness_and_kurtosis()
    //covariance_and_correlation()
    //complexTypeHandle()
    //groupHandle()
    //windowHandle()
    //groupSetHandle()
    //groupMetadata()
    //pivotHandle()
    //udafHandle()
    myUDAF()
  }

  def countHandle(): Unit ={
    //count is actually an action as opposed to a transformation
    print(df.count()) //541909
    import org.apache.spark.sql.functions.count
    df.select(count("StockCode")).show()
    df.selectExpr("count(1)").show()
    //+----------------++--------+
    //|count(StockCode)||count(1)|
    //+----------------++--------+
    //|          541909||  541909|
    //+----------------++--------+
    df.select(countDistinct("StockCode")).show() //4070 took 2.771206 s
    //Aggregate function: returns the approximate number of distinct items in a group.
    //Params:  rsd – maximum estimation error allowed (default = 0.05)
    df.select(approx_count_distinct("StockCode", 0.1)).show() // 3364 took 0.214926 s

    df = loadNullData()
    df.selectExpr("count(1)").show()
    df.select(count("col")).show()
    //+--------++----------+
    //|count(1)||count(col)|
    //+--------++----------+
    //|       5||         2|
    //+--------++----------+

  }

  def first_and_last(): Unit ={
    import org.apache.spark.sql.functions.{first, last}
    df.select(first("StockCode"), last("StockCode")).show()
    //+-----------------------+----------------------+
    //|first(StockCode, false)|last(StockCode, false)|
    //+-----------------------+----------------------+
    //|                 85123A|                 22138|
    //+-----------------------+----------------------+
  }

  def min_and_max(): Unit ={
    import org.apache.spark.sql.functions.{min, max}
    df.select(min("Quantity"), max("Quantity")).show()
    //+-------------+-------------+
    //|min(Quantity)|max(Quantity)|
    //+-------------+-------------+
    //|       -80995|        80995|
    //+-------------+-------------+
  }

  def variance_and_standard_deviation(): Unit ={
    import org.apache.spark.sql.functions.{var_pop, stddev_pop}
    import org.apache.spark.sql.functions.{var_samp, stddev_samp}
    //population and sample
    df.select(var_pop("Quantity"), var_samp("Quantity"),
      stddev_pop("Quantity"), stddev_samp("Quantity")).show()
    //+-----------------+------------------+--------------------+---------------------+
    //|var_pop(Quantity)|var_samp(Quantity)|stddev_pop(Quantity)|stddev_samp(Quantity)|
    //+-----------------+------------------+--------------------+---------------------+
    //|47559.30364660879| 47559.39140929848|  218.08095663447733|   218.08115785023355|
    //+-----------------+------------------+--------------------+---------------------+
  }

  def skewness_and_kurtosis(): Unit ={
    import org.apache.spark.sql.functions.{skewness, kurtosis}
    df.select(skewness("Quantity"), kurtosis("Quantity")).show()
    //+------------------+------------------+
    //|skewness(Quantity)|kurtosis(Quantity)|
    //+------------------+------------------+
    //|-0.264075576105298|119768.05495534067|
    //+------------------+------------------+
  }

  def covariance_and_correlation(): Unit ={
    import org.apache.spark.sql.functions.{corr, covar_pop, covar_samp}
    df.select(corr("InvoiceNo", "Quantity"),
      covar_samp("InvoiceNo", "Quantity"),
      covar_pop("InvoiceNo", "Quantity")).show()
    //+-------------------------+-------------------------------+------------------------------+
    //|corr(InvoiceNo, Quantity)|covar_samp(InvoiceNo, Quantity)|covar_pop(InvoiceNo, Quantity)|
    //+-------------------------+-------------------------------+------------------------------+
    //|     4.912186085636837E-4|             1052.7280543912716|            1052.7260778751674|
    //+-------------------------+-------------------------------+------------------------------+
  }

  def complexTypeHandle(): Unit ={
    // collect a list of values present in a given column or only the unique values by collecting to a set.
    import org.apache.spark.sql.functions.{collect_set, collect_list}
    df.agg(collect_set("Country"), collect_list("Country")).show()
    //+--------------------+---------------------+
    //|collect_set(Country)|collect_list(Country)|
    //+--------------------+---------------------+
    //|[Portugal, Italy,...| [United Kingdom, ...|
    //+--------------------+---------------------+
  }

  def groupHandle(): Unit ={
    df.groupBy("InvoiceNo", "CustomerId").count().show(3,false)
    //+---------+----------+-----+
    //|InvoiceNo|CustomerId|count|
    //+---------+----------+-----+
    //|536846   |14573     |76   |
    //|537026   |12395     |12   |
    //|537883   |14437     |5    |
    //+---------+----------+-----+
    df.groupBy("InvoiceNo").agg(
      count("Quantity").alias("quan"),
      expr("count(Quantity)")).show(3,false)
    //+---------+----+---------------+
    //|InvoiceNo|quan|count(Quantity)|
    //+---------+----+---------------+
    //|   536596|   6|              6|
    //|   536938|  14|             14|
    //|   537252|   1|              1|
    //+---------+----+---------------+
    df.groupBy("InvoiceNo").agg("Quantity"->"avg", "Quantity"->"stddev_pop").show(2)
    //+---------+------------------+--------------------+
    //|InvoiceNo|     avg(Quantity)|stddev_pop(Quantity)|
    //+---------+------------------+--------------------+
    //|   536596|               1.5|  1.1180339887498947|
    //|   536938|33.142857142857146|  20.698023172885524|
    //+---------+------------------+--------------------+
  }

  def groupSetHandle(): Unit ={
    //Get the total quantity of all stock codes and customers

    val dfWithDate = df.withColumn("date", to_date(col("InvoiceDate"),
      "MM/d/yyyy H:mm"))
    dfWithDate.show(2)
    dfWithDate.createOrReplaceTempView("dfWithDate")
    val dfNoNull = dfWithDate.drop()
    dfNoNull.show()
    dfNoNull.createOrReplaceTempView("dfNoNull")
    spark.sql(
      """
        |SELECT CustomerId, stockCode, sum(Quantity) FROM dfNoNull
        |GROUP BY customerId, stockCode
        |ORDER BY CustomerId DESC, stockCode DESC
        |""".stripMargin).show()
    spark.sql(
      """
        |SELECT CustomerId, stockCode, sum(Quantity) FROM dfNoNull
        |GROUP BY GROUPING SETS((customerId, stockCode))
        |ORDER BY CustomerId DESC, stockCode DESC
        |""".stripMargin).show()
    spark.sql(
      """
        |SELECT CustomerId, stockCode, sum(Quantity) FROM dfNoNull
        |GROUP BY GROUPING SETS((customerId, stockCode),())
        |ORDER BY CustomerId DESC, stockCode DESC
        |""".stripMargin).show()
    dfNoNull.rollup("CustomerId", "stockCode").agg(sum("Quantity"))
      .selectExpr("CustomerId", "stockCode", "`sum(Quantity)`")
      .orderBy(desc("CustomerId"))
    dfNoNull.groupBy("CustomerId", "stockCode").agg(sum("Quantity"))
      .selectExpr("CustomerId", "stockCode", "`sum(Quantity)`")
      .orderBy(desc("CustomerId"))
    //+----------+---------+-------------+
    //|CustomerId|stockCode|sum(Quantity)|
    //+----------+---------+-------------+
    //|     18287|    85173|           48|
    //|     18287|   85040A|           48|
    //|     18287|   85039B|          120|
    //+----------+---------+-------------+
    dfNoNull.groupBy("Date", "Country").agg(sum("Quantity"))
      .selectExpr("Date", "Country", "`sum(Quantity)` as total_quantity")
      .orderBy("Date").show()
    //+----------+--------------+--------------+
    //|      Date|       Country|total_quantity|
    //+----------+--------------+--------------+
    //|2010-12-01|          EIRE|           243|
    //|2010-12-01|United Kingdom|         23949|
    //|2010-12-01|        Norway|          1852|
    //|2010-12-01|     Australia|           107|
    //|2010-12-01|       Germany|           117|
    //|2010-12-01|   Netherlands|            97|
    //|2010-12-01|        France|           449|
    //|2010-12-02|United Kingdom|         20873|
    //+----------+--------------+--------------+

    val rolledUpDF = dfNoNull.rollup("Date", "Country").agg(sum("Quantity"))
      .selectExpr("Date", "Country", "`sum(Quantity)` as total_quantity")
      .orderBy("Date")
    rolledUpDF.show()
    //+----------+--------------+--------------+
    //|      Date|       Country|total_quantity|
    //+----------+--------------+--------------+
    //|      null|          null|       5176450|
    //|2010-12-01|          null|         26814|
    //|2010-12-01|     Australia|           107|
    //+----------+--------------+--------------+
    rolledUpDF.where("Country IS NULL").show()
    //+----------+-------+--------------+
    //|      Date|Country|total_quantity|
    //+----------+-------+--------------+
    //|      null|   null|       5176450|
    //|2010-12-01|   null|         26814|
    //|2010-12-02|   null|         21023|
    //+----------+-------+--------------+
    /**
     *  This is a quick and easily accessible summary of nearly all of the information in our table,
     and it’s a great way to create a quick summary table that others can use later on.
     */
    dfNoNull.cube("Date", "Country").agg(sum(col("Quantity")))
      .select("Date", "Country", "sum(Quantity)").orderBy(desc("Date")).show()
    //+----+--------------------+-------------+
    //|null|               Japan|        25218|
    //|null|           Australia|        83653|
    //|null|            Portugal|        16180|
    //|null|             Germany|       117448|
    //|null|                 RSA|          352|
    //|null|           Hong Kong|         4769|
    //|null|              Cyprus|         6317|
    //|null|         Unspecified|         3300|
    //|null|United Arab Emirates|          982|
    //|null|                null|      5176450|
    //+----+--------------------+-------------+
  }

  def groupMetadata(): Unit ={
    import org.apache.spark.sql.functions.{grouping_id, sum, expr}
    val dfWithDate = df.withColumn("date", to_date(col("InvoiceDate"),
      "MM/d/yyyy H:mm"))
    dfWithDate.show(2)
    dfWithDate.createOrReplaceTempView("dfWithDate")
    val dfNoNull = dfWithDate.drop()
    dfNoNull.cube("Date", "Country").agg(sum(col("Quantity")))
      .select("Date", "Country", "sum(Quantity)").orderBy(desc("Date")).show()
    //+----------+---------------+-------------+
    //|      Date|        Country|sum(Quantity)|
    //+----------+---------------+-------------+
    //|2011-12-09|        Belgium|          203|
    //|2011-12-09|         Norway|         2227|
    //|2011-12-09| United Kingdom|         9534|
    //|2011-12-09|         France|          105|
    //|2011-12-09|           null|        12949|
    //|2011-12-09|        Germany|          880|
    //|2011-12-08|           EIRE|          806|
    //|2011-12-08|            USA|         -196|
    //|2011-12-08|         France|           18|
    //|2011-12-08|    Netherlands|          140|
    //+----------+---------------+-------------+
    dfNoNull.cube("Date", "Country").agg(grouping_id(), sum("Quantity"))
      .orderBy(desc("Date"),expr("grouping_id()").asc)
      .show()
    //+----------+---------------+-------------+-------------+
    //|      Date|        Country|grouping_id()|sum(Quantity)|
    //+----------+---------------+-------------+-------------+
    //|2011-12-09|         Norway|            0|         2227|
    //|2011-12-09| United Kingdom|            0|         9534|
    //|2011-12-09|         France|            0|          105|
    //|2011-12-09|        Belgium|            0|          203|
    //|2011-12-09|        Germany|            0|          880|
    //|2011-12-09|           null|            1|        12949| //give us the total quantity on a day, regardless of Country.
    //|2011-12-08|           EIRE|            0|          806|
    //|2011-12-08|        Germany|            0|          969|
    //|2011-12-08|         France|            0|           18|
    //+----------+---------------+-------------+-------------+
  }

  def pivotHandle(): Unit ={
    val dfWithDate = df.withColumn("date", to_date(col("InvoiceDate"),
      "MM/d/yyyy H:mm"))
    val pivoted = dfWithDate.groupBy("date").pivot("Country").sum().show()
  }

  def sumHandle(): Unit ={
    import org.apache.spark.sql.functions.sum
    df.select(sum("Quantity")).show() // 5176450
    //sum a distinct set of values
    df.select(sumDistinct("Quantity")).show() // 29310
  }

  def avgHandle(): Unit ={
    import org.apache.spark.sql.functions.{sum, count, avg, expr}
    df.select(
      count("Quantity").alias("total_transactions"),
      sum("Quantity").alias("total_purchases"),
      avg("Quantity").alias("avg_purchases"),
      expr("mean(Quantity)").alias("mean_purchases"))
      .selectExpr(
        "total_purchases/total_transactions",
        "avg_purchases",
        "mean_purchases").show()
    //+--------------------------------------+----------------+----------------+
    //|(total_purchases / total_transactions)|   avg_purchases|  mean_purchases|
    //+--------------------------------------+----------------+----------------+
    //|                      9.55224954743324|9.55224954743324|9.55224954743324|
    //+--------------------------------------+----------------+----------------+
  }

  def windowHandle(): Unit ={
    import org.apache.spark.sql.functions.{col, to_date}
    val dfWithDate = df.withColumn("date", to_date(col("InvoiceDate"),
      "MM/d/yyyy H:mm"))
    dfWithDate.show(2)
    dfWithDate.createOrReplaceTempView("dfWithDate")

    import org.apache.spark.sql.expressions.Window
    import org.apache.spark.sql.functions.col
    val windowSpec = Window
      .partitionBy("CustomerId", "date")
      .orderBy(col("Quantity").desc)
      .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    import org.apache.spark.sql.functions.max
    val maxPurchaseQuantity = max(col("Quantity")).over(windowSpec)
    //max('Quantity) windowspecdefinition('CustomerId
    // , 'date, 'Quantity DESC NULLS LAST
    // , specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$()))

    //use the dense_rank function to determine which date had the maximum purchase quantity for every customer
    import org.apache.spark.sql.functions.{dense_rank, rank}
    val purchaseDenseRank = dense_rank().over(windowSpec)
    val purchaseRank = rank().over(windowSpec)
    //Window function rank() requires window to be ordered
    dfWithDate.where("CustomerId IS NOT NULL").orderBy("CustomerId")
      .select(
        col("CustomerId"),
        col("date"),
        col("Quantity"),
        purchaseRank.alias("quantityRank"),
        purchaseDenseRank.alias("quantityDenseRank"),
        maxPurchaseQuantity.alias("maxPurchaseQuantity")).show()
    //+----------+----------+--------+------------+-----------------+-------------------+
    //|CustomerId|      date|Quantity|quantityRank|quantityDenseRank|maxPurchaseQuantity|
    //+----------+----------+--------+------------+-----------------+-------------------+
    //|     12346|2011-01-18|   74215|           1|                1|              74215|
    //|     12346|2011-01-18|  -74215|           2|                2|              74215|
    //|     12347|2010-12-07|      36|           1|                1|                 36|
    //|     12347|2010-12-07|      30|           2|                2|                 36|
    //|     12347|2010-12-07|      24|           3|                3|                 36|
    //|     12347|2010-12-07|      12|           4|                4|                 36|
    //|     12347|2010-12-07|      12|           4|                4|                 36|
    //|     12347|2010-12-07|      12|           4|                4|                 36|
    //|     12347|2010-12-07|      12|           4|                4|                 36|
    //|     12347|2010-12-07|      12|           4|                4|                 36|
    //|     12347|2010-12-07|      12|           4|                4|                 36|
    //|     12347|2010-12-07|      12|           4|                4|                 36|
    //|     12347|2010-12-07|      12|           4|                4|                 36|
    //|     12347|2010-12-07|      12|           4|                4|                 36|
    //|     12347|2010-12-07|      12|           4|                4|                 36|
    //|     12347|2010-12-07|      12|           4|                4|                 36|
    //|     12347|2010-12-07|      12|           4|                4|                 36|
    //|     12347|2010-12-07|      12|           4|                4|                 36|
    //|     12347|2010-12-07|       6|          17|                5|                 36|
    //|     12347|2010-12-07|       6|          17|                5|                 36|
    //+----------+----------+--------+------------+-----------------+-------------------+
  }

  def loadData():DataFrame ={
    val df = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("src/data/retail-data/all/*.csv")
      .coalesce(5)
    df.cache()
    df.createOrReplaceTempView("dfTable")
    df
  }

  def loadNullData(): DataFrame ={
    val myManualSchema = new StructType(Array(
      new StructField("some", StringType, true),
      new StructField("col", StringType, true),
      new StructField("names", LongType, true)))
    val myRows = Seq(Row("Hello", null, 1L), Row("Second", "", 2L), Row("Third", null, 3L), Row("Forth", null, null), Row("Normal", "normalCol", 5L))
    val myRDD = spark.sparkContext.parallelize(myRows)
    val myDf = spark.createDataFrame(myRDD, myManualSchema)
    myDf
  }

  def udafHandle(): Unit ={
    val ba = new BoolAnd
    /**
     * this method and the use of UserDefinedAggregateFunction are deprecated.
     * Aggregator[IN, BUF, OUT] should now be registered as a UDF via the functions.udaf(agg) method.
     */
    spark.udf.register("booland", new BoolAnd)
    //+----------+----------+
    //|booland(t)|booland(f)|
    //+----------+----------+
    //|      true|     false|
    //+----------+----------+

    //recommend
    val avgAgg = new Aggregator[Boolean, Boolean, Boolean] {
      //初始值
      override def zero: Boolean = true
      //每个分组区局部聚合的方法，
      override def reduce(b: Boolean, a: Boolean): Boolean = {
        b && a
      }
      //全局聚合调用的方法
      override def merge(b1: Boolean, b2: Boolean): Boolean = {
        b1 && b2
      }
      //计算最终的结果
      override def finish(reduction: Boolean): Boolean = {
        reduction
      }
      //中间结果的encoder
      override def bufferEncoder: Encoder[Boolean] = {
        Encoders.scalaBoolean;
      }
      //返回结果的encoder
      override def outputEncoder: Encoder[Boolean] = {
        Encoders.scalaBoolean
      }
    }
    spark.udf.register("booland", udaf(avgAgg))

    import org.apache.spark.sql.functions._
    spark.range(1)
      .selectExpr("explode(array(TRUE, TRUE, TRUE)) as t")
      .selectExpr("explode(array(TRUE, FALSE, TRUE)) as f", "t")
      .select(ba(col("t")), expr("booland(f)"))
      .show()
    //+----------+---------+
    //|booland(t)|anon$1(f)|
    //+----------+---------+
    //|      true|    false|
    //+----------+---------+

  }

  def myUDAF(): Unit ={
    import spark.implicits._
    val myDF = df.select("InvoiceNo", "Quantity", "UnitPrice").drop().as[Invoice]
    val averageSalary = MyAverage.toColumn.name("average_salary")
    myDF.agg(avg("Quantity")).show()
    //+----------------+
    //|   avg(Quantity)|
    //+----------------+
    //|9.55224954743324|
    //+----------------+
    myDF.agg(avg("UnitPrice")).show()
    //+-----------------+
    //|   avg(UnitPrice)|
    //+-----------------+
    //|4.611113626083471|
    //+-----------------+
    myDF.select(averageSalary).show()
    //+----------------+-----------------+
    //|              _1|               _2|
    //+----------------+-----------------+
    //|9.55224954743324|4.611113626083471|
    //+----------------+-----------------+
    spark.udf.register("myAvg", udaf(MyAverage))
    myDF.createOrReplaceTempView("invoices")
    myDF.selectExpr("myAvg(*)").show(false)
    //+----------------------------------------------------------+
    //|myaverage$(InvoiceNo, CAST(Quantity AS BIGINT), UnitPrice)|
    //+----------------------------------------------------------+
    //|[9.55224954743324, 4.611113626083471]                     |
    //+----------------------------------------------------------+
    spark.sql("SELECT myAvg(*) as average_salary FROM invoices").show(false)
    //+-------------------------------------+
    //|average_salary                       |
    //+-------------------------------------+
    //|[9.55224954743324, 4.611113626083471]|
    //+-------------------------------------+
  }
}
