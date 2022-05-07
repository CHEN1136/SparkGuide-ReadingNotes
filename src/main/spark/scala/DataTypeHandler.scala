package scala

import org.apache.parquet.format.IntType
import org.apache.spark.sql.functions.{col, expr, from_json, lit, lower, round, split, to_json, upper}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.DataFrameHandler.{limitRows, spark}
import scala.UDF.SimpleExample

object DataTypeHandler {

  val spark = SparkSession.builder().master("local").enableHiveSupport().getOrCreate()

  var df: DataFrame = null
  def main(args: Array[String]): Unit = {
    df = loadData()
//    import org.apache.spark.sql.functions.lit
//    df.select(lit(5), lit("five"), lit(5.0)).show()  //lit() create a literal column
    //work_with_boolean()
    //work_with_numbers()
    //work_with_String()
    //regexpHandle()
    //work_with_dates_and_timestamps()
    //work_with_NULL()
    //work_with_ComplexTypes()
    //work_with_JSON()
    work_with_UDF()
  }

  def loadData():DataFrame ={
    val df = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("src/data/retail-data/by-day/2010-12-01.csv")
    df.printSchema()
    df.createOrReplaceTempView("dfTable")
    df
  }

  def work_with_boolean(): Unit ={
    df.where(col("InvoiceNo").equalTo(536365))
      .select("InvoiceNo", "Description")
      .show(5, false)
    df.where(col("InvoiceNo") === 536365)
      .select("InvoiceNo", "Description")
      .show(5, false)
    df.where("InvoiceNo = 536365")
      .show(5, false)

    val priceFilter = col("UnitPrice") > 600
    val describeFilter = col("Description").contains("POSTAGE")
    df.where(col("StockCode").isin("DOT", "71053")).where(priceFilter.or(describeFilter))
      .show()

    val DOTCodeFilter = col("StockCode") === "DOT"
    df.withColumn("isExpensive", DOTCodeFilter.and(priceFilter.or(describeFilter)))
      .where("isExpensive")
      .select("unitPrice", "isExpensive").show(5)
    //+---------+-----------+
    //|unitPrice|isExpensive|
    //+---------+-----------+
    //|   569.77|       true|
    //|   607.49|       true|
    //+---------+-----------+
    import org.apache.spark.sql.functions.{expr, not, col}
    df.withColumn("isExpensive", not(col("UnitPrice").leq(250)))
      .filter("isExpensive")
      .select("Description", "UnitPrice").show(5)
    df.withColumn("isExpensive", expr("NOT UnitPrice <= 250"))
      .filter("isExpensive")
      .select("Description", "UnitPrice").show(5)
    df.where(col("Description").eqNullSafe("hello")).show()
  }

  def work_with_numbers(): Unit ={
    df.select(col("CustomerId"), col("Quantity"), col("UnitPrice")).show(5)
    //+----------+--------+---------+
    //|CustomerId|Quantity|UnitPrice|
    //+----------+--------+---------+
    //|   17850.0|       6|     2.55|
    //|   17850.0|       6|     3.39|
    //|   17850.0|       8|     2.75|
    //|   17850.0|       6|     3.39|
    //|   17850.0|       6|     3.39|
    //+----------+--------+---------+
    import org.apache.spark.sql.functions.{expr, pow}
    val fabricatedQuantity = pow(col("Quantity") * col("UnitPrice"), 2) + 5
    df.select(expr("CustomerId"), fabricatedQuantity.alias("realQuantity")).show(5)
    df.selectExpr(
      "CustomerId",
      "(POWER((Quantity * UnitPrice), 2.0) + 5) as realQuantity").show(5)
    //+----------+------------------+
    //|CustomerId|      realQuantity|
    //+----------+------------------+
    //|   17850.0|239.08999999999997|
    //|   17850.0|          418.7156|
    //|   17850.0|             489.0|
    //|   17850.0|          418.7156|
    //|   17850.0|          418.7156|
    //+----------+------------------+
    df.select(expr("CustomerId"), round(fabricatedQuantity,4).alias("realQuantity")).show(5)
    //+----------+------------+
    //|CustomerId|realQuantity|
    //+----------+------------+
    //|   17850.0|      239.09|
    //|   17850.0|    418.7156|
    //|   17850.0|       489.0|
    //|   17850.0|    418.7156|
    //|   17850.0|    418.7156|
    //+----------+------------+

    import org.apache.spark.sql.functions.{round, bround}
    df.select(round(col("UnitPrice"), 1).alias("rounded"), col("UnitPrice")).show(5)
    //+-------+---------+
    //|rounded|UnitPrice|
    //+-------+---------+
    //|    2.6|     2.55|
    //|    3.4|     3.39|
    //|    2.8|     2.75|
    //|    3.4|     3.39|
    //|    3.4|     3.39|
    //+-------+---------+

    import org.apache.spark.sql.functions.lit
    df.select(round(lit("2.5")), bround(lit("2.5"))).show(2)
    //+-------------+--------------+
    //|round(2.5, 0)|bround(2.5, 0)|
    //+-------------+--------------+
    //|          3.0|           2.0|
    //|          3.0|           2.0|
    //+-------------+--------------+

    import org.apache.spark.sql.functions.{corr}
    println(df.stat.corr("Quantity", "UnitPrice"))  //-0.04112314436835551
    df.select(corr("Quantity", "UnitPrice")).show()

    df.select(col("StockCode"),col("Description"),col("Description")).describe().show()
    //+-------+------------------+--------------------+--------------------+
    //|summary|         StockCode|         Description|         Description|
    //+-------+------------------+--------------------+--------------------+
    //|  count|              3108|                3098|                3098|
    //|   mean|27834.304044117645|                null|                null|
    //| stddev|17407.897548583845|                null|                null|
    //|    min|             10002| 4 PURPLE FLOCK D...| 4 PURPLE FLOCK D...|
    //|    max|              POST|ZINC WILLIE WINKI...|ZINC WILLIE WINKI...|
    //+-------+------------------+--------------------+--------------------+
    val quantileProbs = Array(0.5)
    val relError = 0.05
    df.stat.approxQuantile("UnitPrice", quantileProbs, relError) //2.51

    import org.apache.spark.sql.functions.monotonically_increasing_id
    df.select(monotonically_increasing_id().alias("id"), expr("*")).show(5)
    //+---+---------+---------+--------------------+--------+-------------------+---------+----------+--------------+
    //| id|InvoiceNo|StockCode|         Description|Quantity|        InvoiceDate|UnitPrice|CustomerID|       Country|
    //+---+---------+---------+--------------------+--------+-------------------+---------+----------+--------------+
    //|  0|   536365|   85123A|WHITE HANGING HEA...|       6|2010-12-01 08:26:00|     2.55|   17850.0|United Kingdom|
    //|  1|   536365|    71053| WHITE METAL LANTERN|       6|2010-12-01 08:26:00|     3.39|   17850.0|United Kingdom|
    //|  2|   536365|   84406B|CREAM CUPID HEART...|       8|2010-12-01 08:26:00|     2.75|   17850.0|United Kingdom|
    //|  3|   536365|   84029G|KNITTED UNION FLA...|       6|2010-12-01 08:26:00|     3.39|   17850.0|United Kingdom|
    //|  4|   536365|   84029E|RED WOOLLY HOTTIE...|       6|2010-12-01 08:26:00|     3.39|   17850.0|United Kingdom|
    //+---+---------+---------+--------------------+--------+-------------------+---------+----------+--------------+
  }

  def work_with_String(): Unit ={
    import org.apache.spark.sql.functions.{initcap}
    df.select("Description").show(2, false)
    //+----------------------------------+
    //|Description                       |
    //+----------------------------------+
    //|WHITE HANGING HEART T-LIGHT HOLDER|
    //|WHITE METAL LANTERN               |
    //+----------------------------------+
    df.select(initcap(col("Description")),
      lower(col("Description")),
      upper(col("Description")),

    ).show(2, false)
    //+----------------------------------+----------------------------------+----------------------------------+
    //|initcap(Description)              |lower(Description)                |upper(Description)                |
    //+----------------------------------+----------------------------------+----------------------------------+
    //|White Hanging Heart T-light Holder|white hanging heart t-light holder|WHITE HANGING HEART T-LIGHT HOLDER|
    //|White Metal Lantern               |white metal lantern               |WHITE METAL LANTERN               |
    //+----------------------------------+----------------------------------+----------------------------------+
    import org.apache.spark.sql.functions.{lit, ltrim, rtrim, rpad, lpad, trim}
    df.select(
      ltrim(lit(" HELLO ")).as("ltrim"),
      rtrim(lit(" HELLO ")).as("rtrim"),
      trim(lit(" HELLO ")).as("trim"),
      lpad(lit("HELLO"), 6, " ").as("lp"),
      rpad(lit("HELLO"), 6, " ").as("rp")).show(2,false)
    //lp len = 3, rp len = 10
    // +------+------+-----+---+----------+
    //| ltrim| rtrim| trim| lp|        rp|
    //+------+------+-----+---+----------+
    //|HELLO | HELLO|HELLO|HEL|HELLO     |
    //|HELLO | HELLO|HELLO|HEL|HELLO     |
    //+------+------+-----+---+----------+
    //lp len = 6, rp len = 6
    // +------+------+-----+------+------+
    //|ltrim |rtrim |trim |lp    |rp    |
    //+------+------+-----+------+------+
    //|HELLO | HELLO|HELLO| HELLO|HELLO |
    //|HELLO | HELLO|HELLO| HELLO|HELLO |
    //+------+------+-----+------+------+
  }

  def work_with_dates_and_timestamps(): Unit ={
    import org.apache.spark.sql.functions.{current_date, current_timestamp}
    val dateDF = spark.range(10)
      .withColumn("today", current_date())
      .withColumn("now", current_timestamp())
    dateDF.show(false)
    dateDF.printSchema()
    //+---+----------+-----------------------+
    //|id |today     |now                    |
    //+---+----------+-----------------------+
    //|0  |2022-04-30|2022-04-30 15:26:13.822|
    //|1  |2022-04-30|2022-04-30 15:26:13.822|
    //|2  |2022-04-30|2022-04-30 15:26:13.822|
    //|3  |2022-04-30|2022-04-30 15:26:13.822|
    //|4  |2022-04-30|2022-04-30 15:26:13.822|
    //|5  |2022-04-30|2022-04-30 15:26:13.822|
    //|6  |2022-04-30|2022-04-30 15:26:13.822|
    //|7  |2022-04-30|2022-04-30 15:26:13.822|
    //|8  |2022-04-30|2022-04-30 15:26:13.822|
    //|9  |2022-04-30|2022-04-30 15:26:13.822|
    //+---+----------+-----------------------+
    //root
    // |-- id: long (nullable = false)
    // |-- today: date (nullable = false)
    // |-- now: timestamp (nullable = false)
    dateDF.createOrReplaceTempView("dateTable")
    import org.apache.spark.sql.functions.{date_add, date_sub}
    dateDF.select(date_sub(col("today"), 5), date_add(col("today"), 5)).show(1)
    //+------------------+------------------+
    //|date_sub(today, 5)|date_add(today, 5)|
    //+------------------+------------------+
    //|        2022-04-25|        2022-05-05|
    //+------------------+------------------+
    import org.apache.spark.sql.functions.{datediff, months_between, to_date}
    dateDF.withColumn("week_ago", date_sub(col("today"), 7))
      .select(datediff(col("week_ago"), col("today"))).show(1)
    //+-------------------------+
    //|datediff(week_ago, today)|
    //+-------------------------+
    //|                       -7|
    //+-------------------------+
    dateDF.select(
      to_date(lit("2016-01-01")).alias("start"),
      to_date(lit("2017-05-22")).alias("end"))
      .select(months_between(col("start"), col("end"),false)).show(1)
    //+--------------------------------++---------------------------------+
    //|months_between(start, end, true)||months_between(start, end, false)|
    //+--------------------------------++---------------------------------+
    //|                    -16.67741935||              -16.677419354838708|
    //+--------------------------------++---------------------------------+

    import org.apache.spark.sql.functions.to_date
    val dateFormat = "yyyy-dd-MM"
    val cleanDateDF = spark.range(1).select(
      to_date(lit("2017-12-11"), dateFormat).alias("date"),
      to_date(lit("2017-20-12"), dateFormat).alias("date2"))
    cleanDateDF.createOrReplaceTempView("dateTable2")
    cleanDateDF.show()
    //+----------+----------+
    //|      date|     date2|
    //+----------+----------+
    //|2017-11-12|2017-12-20|
    //+----------+----------+
    cleanDateDF.printSchema()
    //root
    // |-- date: date (nullable = true)
    // |-- date2: date (nullable = true)
    import org.apache.spark.sql.functions.to_timestamp
    cleanDateDF.select(to_timestamp(col("date"), dateFormat)).show()
    //+----------------------------------+
    //|to_timestamp(`date`, 'yyyy-dd-MM')|
    //+----------------------------------+
    //|               2017-11-12 00:00:00|
    //+----------------------------------+
    cleanDateDF.filter(col("date2") > lit("2017-12-12")).show()
    cleanDateDF.filter(col("date2") > to_date(lit("2017-12-12"))).show()
    cleanDateDF.filter(col("date2") > "2017-12-12").show()
    cleanDateDF.filter(col("date2") > "'2017-12-12'").show() //failed
  }

  def work_with_NULL(): Unit ={
    import org.apache.spark.sql.functions.coalesce
    df = loadNullData()
    df.show()
    //+------+---------+-----+
    //|  some|      col|names|
    //+------+---------+-----+
    //| Hello|     null|    1|
    //|Second|         |    2|
    //| Third|     null|    3|
    //| Forth|     null| null|
    //|Normal|normalCol|    5|
    //+------+---------+-----+
    df.select(coalesce(col("some"), col("col"))).show(false)
    //+-------------------+
    //|coalesce(some, col)|
    //+-------------------+
    //|Hello              |
    //|Second             |
    //|Third              |
    //|Forth              |
    //|Normal             |
    //+-------------------+
    //removes rows that contain nulls. The default is to drop any row in which any value is null
    df.na.drop().show()
    //+------+---------+-----+
    //|  some|      col|names|
    //+------+---------+-----+
    //|Second|         |    2|
    //|Normal|normalCol|    5|
    //+------+---------+-----+
    df.na.drop("any").show()
    //+------+---------+-----+
    //|  some|      col|names|
    //+------+---------+-----+
    //|Second|         |    2|
    //|Normal|normalCol|    5|
    //+------+---------+-----+

    // Using “all” drops the row only if all values are null or NaN for that row
    df.na.drop("all").show()
    //+------+---------+-----+
    //|  some|      col|names|
    //+------+---------+-----+
    //| Hello|     null|    1|
    //|Second|         |    2|
    //| Third|     null|    3|
    //| Forth|     null| null|
    //|Normal|normalCol|    5|
    //+------+---------+-----+

    //certain sets of columns by passing in an array of columns
    df.na.drop("all", Seq("col", "names")).show()
    //+------+---------+-----+
    //|  some|      col|names|
    //+------+---------+-----+
    //| Hello|     null|    1|
    //|Second|         |    2|
    //| Third|     null|    3|
    //|Normal|normalCol|    5|
    //+------+---------+-----+

    //fill all null values in column
    df.na.fill("All Null values become this string").show(false)
    //+------+----------------------------------+-----+
    //|some  |col                               |names|
    //+------+----------------------------------+-----+
    //|Hello |All Null values become this string|1    |
    //|Second|                                  |2    |
    //|Third |All Null values become this string|3    |
    //|Forth |All Null values become this string|null |
    //|Normal|normalCol                         |5    |
    //+------+----------------------------------+-----+
    df.na.fill(5, Seq("col", "names")).show()
    df.na.fill("fill", Seq("col", "names")).show()
    //+------+---------+-----++------+---------+-----+
    //|  some|      col|names||  some|      col|names|
    //+------+---------+-----++------+---------+-----+
    //| Hello|     null|    1|| Hello|     fill|    1|
    //|Second|         |    2||Second|         |    2|
    //| Third|     null|    3|| Third|     fill|    3|
    //| Forth|     null|    5|| Forth|     fill| null|
    //|Normal|normalCol|    5||Normal|normalCol|    5|
    //+------+---------+-----++------+---------+-----+

    val fillColValues = Map("names" -> 5, "col" -> "No Value") //recommend
    df.na.fill(fillColValues).show()
    //+------+---------+-----+
    //|  some|      col|names|
    //+------+---------+-----+
    //| Hello| No Value|    1|
    //|Second|         |    2|
    //| Third| No Value|    3|
    //| Forth| No Value|    5|
    //|Normal|normalCol|    5|
    //+------+---------+-----+

    //Probably the most common use case is to replace all values in a certain column according to their current value.
    df.na.replace("col", Map("" -> "UNKNOWN")).show()
    //+------+---------+-----+
    //|  some|      col|names|
    //+------+---------+-----+
    //| Hello|     null|    1|
    //|Second|  UNKNOWN|    2|
    //| Third|     null|    3|
    //| Forth|     null| null|
    //|Normal|normalCol|    5|
    //+------+---------+-----+
    df.orderBy(col("names").desc).show()
    //+------+---------+-----+
    //|  some|      col|names|
    //+------+---------+-----+
    //|Normal|normalCol|    5|
    //| Third|     null|    3|
    //|Second|         |    2|
    //| Hello|     null|    1|
    //| Forth|     null| null|
    //+------+---------+-----+
    df.orderBy(col("names").desc_nulls_first).show()
    //+------+---------+-----+
    //|  some|      col|names|
    //+------+---------+-----+
    //| Forth|     null| null|
    //|Normal|normalCol|    5|
    //| Third|     null|    3|
    //|Second|         |    2|
    //| Hello|     null|    1|
    //+------+---------+-----+
    df.orderBy(col("names").asc_nulls_last).show()
    //+------+---------+-----+
    //|  some|      col|names|
    //+------+---------+-----+
    //| Hello|     null|    1|
    //|Second|         |    2|
    //| Third|     null|    3|
    //|Normal|normalCol|    5|
    //| Forth|     null| null|
    //+------+---------+-----+
  }

  def work_with_ComplexTypes(): Unit ={
    //structTypeHandle()
    //arrayTypeHandle()
    mapTypeHandle()
  }

  def work_with_JSON(): Unit ={
    val jsonDF = spark.range(1)
      .selectExpr("""'{"myJSONKey" : {"myJSONValue" : [1, 2, 3]}}' as jsonString""")
    jsonDF.show(false)
    //+-------------------------------------------+
    //|jsonString                                 |
    //+-------------------------------------------+
    //|{"myJSONKey" : {"myJSONValue" : [1, 2, 3]}}|
    //+-------------------------------------------+

    import org.apache.spark.sql.functions.{get_json_object, json_tuple}
    jsonDF.select(
      get_json_object(col("jsonString"), "$.myJSONKey.myJSONValue[1]") as "column",
      json_tuple(col("jsonString"), "myJSONKey")).show(2,false)
    //+------+-----------------------+
    //|column|c0                     |
    //+------+-----------------------+
    //|2     |{"myJSONValue":[1,2,3]}|
    //+------+-----------------------+
    jsonDF.selectExpr(
      "get_json_object(jsonString, '$.myJSONKey.myJSONValue[1]') as column"
      , "json_tuple(jsonString, 'myJSONKey')").show(2,false)
    //+------+-----------------------+
    //|column|c0                     |
    //+------+-----------------------+
    //|2     |{"myJSONValue":[1,2,3]}|
    //+------+-----------------------+
    //turn a StructType into a JSON string by using the to_json function

    import org.apache.spark.sql.functions.to_json
    df.selectExpr("(InvoiceNo, Description) as myStruct")
      .select(to_json(col("myStruct"))).show(2,false)
    //+-------------------------------------------------------------------------+
    //|to_json(myStruct)                                                        |
    //+-------------------------------------------------------------------------+
    //|{"InvoiceNo":"536365","Description":"WHITE HANGING HEART T-LIGHT HOLDER"}|
    //|{"InvoiceNo":"536365","Description":"WHITE METAL LANTERN"}               |
    //+-------------------------------------------------------------------------+
    /**
     * You can use the from_json function to parse this (or other JSON data) back in.
     * This naturally requires you to specify a schema, and optionally you can specify a map of options
     */
    import org.apache.spark.sql.functions.from_json
    import org.apache.spark.sql.types._
    val parseSchema = new StructType(Array(
      new StructField("InvoiceNo",StringType,true),
      new StructField("Description",StringType,true)))
    df.selectExpr("(InvoiceNo, Description) as myStruct")
      .select(to_json(col("myStruct")).alias("newJSON"))
      .select(from_json(col("newJSON"), parseSchema), col("newJSON")).show(2,false)
    //+--------------------------------------------+-------------------------------------------------------------------------+
    //|from_json(newJSON)                          |newJSON                                                                  |
    //+--------------------------------------------+-------------------------------------------------------------------------+
    //|[536365, WHITE HANGING HEART T-LIGHT HOLDER]|{"InvoiceNo":"536365","Description":"WHITE HANGING HEART T-LIGHT HOLDER"}|
    //|[536365, WHITE METAL LANTERN]               |{"InvoiceNo":"536365","Description":"WHITE METAL LANTERN"}               |
    //+--------------------------------------------+-------------------------------------------------------------------------+
  }

  def work_with_UDF(): Unit ={
    // in Scala
    val udfExampleDF = spark.range(5).toDF("num")
    println(power3(2.0))
    import org.apache.spark.sql.functions.udf
    //val power3udf = udf(power3(_:Double):Double)
    val power3udf = udf(SimpleExample.power(_:Double):Double)
    //val power3udf = udf(SimpleExample.power(_))  //not recommend
    udfExampleDF.select(power3udf(col("num"))).show()
    //+--------+
    //|UDF(num)|
    //+--------+
    //|     0.0|
    //|     1.0|
    //|     8.0|
    //|    27.0|
    //|    64.0|
    //+--------+
    /**
        At this juncture, we can use this only as a DataFrame function. That is to say, we can’t use it
        within a string expression, only on an expression. However, we can also register this UDF as a
        Spark SQL function. This is valuable because it makes it simple to use this function within SQL
        as well as across languages.
     */
    //register the function, 对df api不可见
    spark.udf.register("power3", power3(_:Double):Double)
    udfExampleDF.selectExpr("power3(num)").show(2)
    udfExampleDF.createOrReplaceTempView("udf_table")

    //所谓持久化，外部引入方法
    val sqlS = "CREATE OR REPLACE FUNCTION pow3 AS 'MyUDF' USING JAR 'src/data/udf-1.0-SNAPSHOT.jar'"
    spark.sql(sqlS)
    val exeS = "SELECT pow3(num) AS function_return_value FROM udf_table"
    spark.sql(exeS).show()
  }

  def structTypeHandle(): Unit ={
    import org.apache.spark.sql.functions.struct
    //val complexDF = df.select(struct("Description", "InvoiceNo").alias("complex"))
    //val complexDF = df.selectExpr("struct(Description, InvoiceNo) as complex")
    val complexDF = df.selectExpr("(Description, InvoiceNo) as complex")
    complexDF.createOrReplaceTempView("complexDF")
    complexDF.show(2,false)
    //+--------------------------------------------+
    //|complex                                     |
    //+--------------------------------------------+
    //|[WHITE HANGING HEART T-LIGHT HOLDER, 536365]|
    //|[WHITE METAL LANTERN, 536365]               |
    //+--------------------------------------------+
    complexDF.select("complex.Description").show(2,false)
    complexDF.select(col("complex").getField("Description")).show(2,false)
    //+----------------------------------+
    //|Description                       |
    //+----------------------------------+
    //|WHITE HANGING HEART T-LIGHT HOLDER|
    //|WHITE METAL LANTERN               |
    //+----------------------------------+
    complexDF.select("complex.*").show(2,false)
    //+----------------------------------+---------+
    //|Description                       |InvoiceNo|
    //+----------------------------------+---------+
    //|WHITE HANGING HEART T-LIGHT HOLDER|536365   |
    //|WHITE METAL LANTERN               |536365   |
    //+----------------------------------+---------+
  }

  def arrayTypeHandle(): Unit ={
    import org.apache.spark.sql.functions.split
    //using the split function and specify the delimiter
    df.select(split(col("Description"), " ")).show(2,false)
    //+----------------------------------------+
    //|split(Description,  , -1)               |
    //+----------------------------------------+
    //|[WHITE, HANGING, HEART, T-LIGHT, HOLDER]|
    //|[WHITE, METAL, LANTERN]                 |
    //+----------------------------------------+
    /**
     特别地， 如果切割列为null, 则切割后的array也为null
     如： df.select(split(col("col"), " ").alias("array_col")).selectExpr("*").show()
            +-----------+
            |  array_col|
            +-----------+
            |       null|
            |         []|
            |       null|
            |       null|
            |[normalCol]|
            +-----------+
     */
    //query the values of the array using Python-like syntax
    df.select(split(col("Description"), " ").alias("array_col"))
      .selectExpr("array_col[4]").show(2)
    //+------------+
    //|array_col[4]|
    //+------------+
    //|      HOLDER|
    //|        null|
    //+------------+
    //querying its size
    import org.apache.spark.sql.functions.size
    df.select(size(split(col("Description"), " "))).show(2)
    //+-------------------------------+
    //|size(split(Description,  , -1))|
    //+-------------------------------+
    //|                              5|
    //|                              3|
    //+-------------------------------+
    //see whether this array contains a value
    import org.apache.spark.sql.functions.array_contains
    df.select(array_contains(split(col("Description"), " "), "HEART")).show(2)
    //需要完全匹配， 例如指定value = “H”， 则全为false
    //+------------------------------------------------+
    //|array_contains(split(Description,  , -1), HEART)|
    //+------------------------------------------------+
    //|                                            true|
    //|                                           false|
    //+------------------------------------------------+
    import org.apache.spark.sql.functions.{split, explode}
    df.withColumn("splitted", split(col("Description"), " "))
      .withColumn("exploded", explode(col("splitted")))
      .select("Description", "InvoiceNo", "exploded").show(2)
    //+--------------------+---------+--------+
    //|         Description|InvoiceNo|exploded|
    //+--------------------+---------+--------+
    //|WHITE HANGING HEA...|   536365|   WHITE|
    //|WHITE HANGING HEA...|   536365| HANGING|
    //+--------------------+---------+--------+
  }

  def mapTypeHandle(): Unit ={
    import org.apache.spark.sql.functions.map
    df.select(map(col("Description"), col("InvoiceNo"))
      .alias("complex_map")).show(2,false)
    //+--------------------+
    //| complex_map|
    //+--------------------+
    //|Map(WHITE HANGING...|
    //|Map(WHITE METAL L...|
    //+--------------------+
    df.select(map(col("Description"), col("InvoiceNo")).alias("complex_map"))
      .selectExpr("complex_map['WHITE METAL LANTERN']").show(2)
    //+--------------------------------+
    //|complex_map[WHITE METAL LANTERN]|
    //+--------------------------------+
    //| null|
    //| 536365|
    //+--------------------------------+
    df.select(map(col("Description"), col("InvoiceNo")).alias("complex_map"))
      .selectExpr("explode(complex_map)").show(2)
    //+--------------------+------+
    //|                 key| value|
    //+--------------------+------+
    //|WHITE HANGING HEA...|536365|
    //| WHITE METAL LANTERN|536365|
    //+--------------------+------+
  }

  def regexpHandle(): Unit ={
    // in Scala
    import org.apache.spark.sql.functions.regexp_replace
    val simpleColors = Seq("black", "white", "red", "green", "blue")
    //regexString = BLACK|WHITE|RED|GREEN|BLUE
    var regexString = simpleColors.map(_.toUpperCase).mkString("|")
    // the | signifies `OR` in regular expression syntax
    df.select(
      regexp_replace(col("Description"), regexString, "COLOR").alias("color_clean"),
      col("Description")).show(2,false)
    //+----------------------------------+----------------------------------+
    //|color_clean                       |Description                       |
    //+----------------------------------+----------------------------------+
    //|COLOR HANGING HEART T-LIGHT HOLDER|WHITE HANGING HEART T-LIGHT HOLDER|
    //|COLOR METAL LANTERN               |WHITE METAL LANTERN               |
    //+----------------------------------+----------------------------------+
    import org.apache.spark.sql.functions.translate
    df.select(translate(col("Description"), "LEET", "1337"), col("Description"))
      .show(2)
    //+----------------------------------+--------------------+
    //|translate(Description, LEET, 1337)|         Description|
    //+----------------------------------+--------------------+
    //|              WHI73 HANGING H3A...|WHITE HANGING HEA...|
    //|               WHI73 M37A1 1AN73RN| WHITE METAL LANTERN|
    //+----------------------------------+--------------------+
    import org.apache.spark.sql.functions.regexp_extract
    regexString = simpleColors.map(_.toUpperCase).mkString("(", "|", ")")
    // the | signifies OR in regular expression syntax
    df.select(
      regexp_extract(col("Description"), regexString, 1).alias("color_clean"),
      col("Description")).show(2,false)
    //+-----------+----------------------------------+
    //|color_clean|Description                       |
    //+-----------+----------------------------------+
    //|WHITE      |WHITE HANGING HEART T-LIGHT HOLDER|
    //|WHITE      |WHITE METAL LANTERN               |
    //+-----------+----------------------------------+
    val containsPURPLE = col("Description").contains("PURPLE")
    val containsWhite = col("DESCRIPTION").contains("WHITE")
    df.withColumn("hasSimpleColor", containsPURPLE.or(containsWhite))
      .where("hasSimpleColor")
      .select("Description").show(2, false)
    //+----------------------------------+
    //|Description                       |
    //+----------------------------------+
    //|WHITE HANGING HEART T-LIGHT HOLDER|
    //|WHITE METAL LANTERN               |
    //+----------------------------------+
    df.withColumn("hasSimpleColor", containsPURPLE)
      .where("hasSimpleColor")
      .select("Description").show(2, false)
    //+-----------------------------------+
    //|Description                        |
    //+-----------------------------------+
    //|PURPLE DRAWERKNOB ACRYLIC EDWARDIAN|
    //|FELTCRAFT HAIRBAND PINK AND PURPLE |
    //+-----------------------------------+

    val selectedColumns = simpleColors.map(color => {
      col("Description").contains(color.toUpperCase).alias(s"is_$color")
    }) // could also append this value
    df.select(selectedColumns:_*).where(col("is_white").or(col("is_red")))
      .show(3, false)
    //+--------+--------+------+--------+-------+
    //|is_black|is_white|is_red|is_green|is_blue|
    //+--------+--------+------+--------+-------+
    //|false   |true    |false |false   |false  |
    //|false   |true    |false |false   |false  |
    //|false   |true    |true  |false   |false  |
    //+--------+--------+------+--------+-------+
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

  def power3(number:Double):Double = number * number * number

}

