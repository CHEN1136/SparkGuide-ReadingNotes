package scala

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SQLHandler {
  val spark = SparkSession.builder().master("local").enableHiveSupport().getOrCreate()

  def main(args: Array[String]): Unit ={
    //simpleHandle()
    //createTable()
    //createExternalTable()
    //insert()
    //describeMetadata()
    //deleteTable()
    //createView()
    //dropView()
    //databaseHandle()
    //showGlobalTemp()
    //selectStatement()
    //complexTypeHandle()
    //functionHandle()
    //subQueryHandle()
    configuration()
  }

  def simpleHandle(): Unit ={
    spark.sql("SELECT 1 + 1").show()
    //+-------+
    //|(1 + 1)|
    //+-------+
    //|      2|
    //+-------+
    spark.read.json("src/data/flight-data/json/2015-summary.json")
      .createOrReplaceTempView("some_sql_view") // DF => SQL
    spark.sql("""
SELECT DEST_COUNTRY_NAME, sum(count)
FROM some_sql_view GROUP BY DEST_COUNTRY_NAME
""")
      .where("DEST_COUNTRY_NAME like 'S%'").where("`sum(count)` > 10")
      .show() // SQL => DF
    //+--------------------+----------+
    //|   DEST_COUNTRY_NAME|sum(count)|
    //+--------------------+----------+
    //|             Senegal|        40|
    //|              Sweden|       118|
    //|               Spain|       420|
    //|    Saint Barthelemy|        39|
    //|Saint Kitts and N...|       139|
    //|         South Korea|      1048|
    //|        Sint Maarten|       325|
    //|        Saudi Arabia|        83|
    //|         Switzerland|       294|
    //|         Saint Lucia|       123|
    //|               Samoa|        25|
    //|        South Africa|        36|
    //+--------------------+----------+
  }

  def createTable(): Unit ={
    val sql = """CREATE TABLE flights (
                |DEST_COUNTRY_NAME STRING, ORIGIN_COUNTRY_NAME STRING, count LONG)
                |USING JSON OPTIONS (path 'src/data/flight-data/json/2015-summary.json')
                |""".stripMargin
    spark.sql(sql)
    spark.sql("""CREATE TABLE flights_csv (
                |DEST_COUNTRY_NAME STRING,
                |ORIGIN_COUNTRY_NAME STRING COMMENT "remember, the US will be most prevalent",
                |count LONG)
                |USING csv OPTIONS (header true, path 'src/data/flight-data/csv/2015-summary.csv')
                |""".stripMargin)
    spark.sql("""CREATE TABLE flights_from_select USING parquet AS SELECT * FROM flights""")
    spark.sql("""CREATE TABLE partitioned_flights USING parquet PARTITIONED BY (DEST_COUNTRY_NAME)
                |AS SELECT DEST_COUNTRY_NAME, ORIGIN_COUNTRY_NAME, count FROM flights LIMIT 5""".stripMargin)

    spark.sql(
      """
        |SELECT * FROM flights
        |""".stripMargin).show(5,false)
    spark.sql(
      """
        |SELECT * FROM flights_csv
        |""".stripMargin).show(5,false)
    spark.sql(
      """
        |SELECT * FROM flights_from_select
        |""".stripMargin).show(5,false)

    spark.sql(
      """
        |SELECT * FROM partitioned_flights
        |""".stripMargin).show(6,false)
    //+-----------------+-------------------+-----+
    //|DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|
    //+-----------------+-------------------+-----+
    //|United States    |Romania            |15   |
    //|United States    |Croatia            |1    |
    //|United States    |Ireland            |344  |
    //|Egypt            |United States      |15   |
    //|United States    |India              |62   |
    //+-----------------+-------------------+-----+
  }

  def createExternalTable(): Unit ={
    spark.sql("""drop table IF EXISTS hive_flights""")
    spark.sql("""drop table IF EXISTS hive_flights_2""")
    spark.sql("""CREATE EXTERNAL TABLE hive_flights (
                |      DEST_COUNTRY_NAME STRING, ORIGIN_COUNTRY_NAME STRING, count LONG)
                |    ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION 'src/data/flight-data-hive'""".stripMargin)
    spark.sql("""CREATE EXTERNAL TABLE hive_flights_2
                |ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
                |LOCATION 'src/data/flight-data-hive' AS SELECT * FROM flights
                |""".stripMargin)
    /**
     * 执行以下查询会跳转到创建外部表的table路径，一定要确保文件的权限
     */
    spark.sql("Select * From hive_flights").show(5,false)
    spark.sql("Select * From hive_flights_2").show(5,false)
    //+-----------------+-------------------+-----+
    //|DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|
    //+-----------------+-------------------+-----+
    //|United States    |Romania            |15   |
    //|United States    |Croatia            |1    |
    //|United States    |Ireland            |344  |
    //|Egypt            |United States      |15   |
    //|United States    |India              |62   |
    //+-----------------+-------------------+-----+
  }

  def insert(): Unit ={
    spark.sql("""CREATE TABLE IF NOT EXISTS flights_empty (
                |DEST_COUNTRY_NAME STRING,
                |ORIGIN_COUNTRY_NAME STRING,
                |count LONG)
                |""".stripMargin)

    spark.sql(
      """
        |INSERT INTO flights_empty
        |SELECT DEST_COUNTRY_NAME, ORIGIN_COUNTRY_NAME, count FROM flights ORDER BY count desc LIMIT 20
        |""".stripMargin)
    spark.sql(
      """
        |select * from flights_empty
        |""".stripMargin).show(5, false)
    //+-----------------+-------------------+------+
    //|DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count |
    //+-----------------+-------------------+------+
    //|United States    |United States      |370002|
    //|United States    |Canada             |8483  |
    //|Canada           |United States      |8399  |
    //|United States    |Mexico             |7187  |
    //|Mexico           |United States      |7140  |
    //+-----------------+-------------------+------+
  }

  def describeMetadata(): Unit ={
    spark.sql("""DESCRIBE TABLE flights_csv
                |""".stripMargin).show(false)
    //+-------------------+---------+---------------------------------------+
    //|col_name           |data_type|comment                                |
    //+-------------------+---------+---------------------------------------+
    //|DEST_COUNTRY_NAME  |string   |null                                   |
    //|ORIGIN_COUNTRY_NAME|string   |remember, the US will be most prevalent|
    //|count              |bigint   |null                                   |
    //+-------------------+---------+---------------------------------------+
    spark.sql("""SHOW PARTITIONS partitioned_flights
                |""".stripMargin).show(false)
    //+---------------------------------+
    //|partition                        |
    //+---------------------------------+
    //|DEST_COUNTRY_NAME=Egypt          |
    //|DEST_COUNTRY_NAME=United%20States|
    //+---------------------------------+
  }

  def deleteTable(): Unit ={
    spark.sql("""drop table hive_flights""")
    spark.sql("""drop table if exists hive_flights_2""")
    try{
      /**
       * org.apache.spark.sql.AnalysisException: Table or view not found: hive_flights; line 1 pos 14;
      'Project [*]
      +- 'UnresolvedRelation [hive_flights]
       */
      //spark.sql("Select * From hive_flights").show(5,false)

      /**
       *org.apache.spark.sql.AnalysisException: Table or view not found: hive_flights_2; line 1 pos 14;
        'Project [*]
        +- 'UnresolvedRelation [hive_flights_2]
       */
      spark.sql("Select * From hive_flights_2").show(5,false)
    }catch{
      case e: Throwable => println(e)
    }
  }

  def createView(): Unit ={
    spark.sql("""
                |CREATE VIEW just_usa_view AS
                |SELECT * FROM flights WHERE dest_country_name = 'United States'
                |""".stripMargin)
    /**
     * Like tables, you can create temporary views
     * that are available only during the current session and are not registered to a database
     */
    spark.sql(
      """
        |CREATE TEMP VIEW just_usa_view_temp AS
        |SELECT * FROM flights WHERE dest_country_name = 'United States'
        |""".stripMargin)

    /**
     * Global temp views are resolved regardless of database and are viewable across the entire Spark application,
     * but they are removed at the end of the session
     */
    spark.sql(
      """
        |CREATE GLOBAL TEMP VIEW just_usa_global_view_temp AS
        |SELECT * FROM flights WHERE dest_country_name = 'United States'
        |""".stripMargin)
    spark.sql("SHOW TABLES").show()
    //+--------+-------------------+-----------+
    //|database|          tableName|isTemporary|
    //+--------+-------------------+-----------+
    //| default|      bucketedfiles|      false|
    //| default|            flights|      false|
    //| default|        flights_csv|      false|
    //| default|      flights_empty|      false|
    //| default|flights_from_select|      false|
    //| default|       hive_flights|      false|
    //| default|      just_usa_view|      false|
    //| default|partitioned_flights|      false|
    //|        | just_usa_view_temp|       true|
    //+--------+-------------------+-----------+
    /**
     * overwrite a view
     */
    spark.sql(
      """
        |CREATE OR REPLACE TEMP VIEW just_usa_view_temp AS
        |SELECT * FROM flights WHERE dest_country_name = 'United States'
        |""".stripMargin)
    spark.sql(
      """
        |SELECT * FROM just_usa_view_temp
        |""".stripMargin).show()
    //+-----------------+--------------------+-----+
    //|DEST_COUNTRY_NAME| ORIGIN_COUNTRY_NAME|count|
    //+-----------------+--------------------+-----+
    //|    United States|             Romania|   15|
    //|    United States|              Russia|  161|
    //|       .................................... |
    //|    United States|          Costa Rica|  608|
    //+-----------------+--------------------+-----+
    spark.sql(
      """
        |SELECT * FROM global_temp.just_usa_global_view_temp
        |""".stripMargin).show()
    //+-----------------+--------------------+-----+
    //|DEST_COUNTRY_NAME| ORIGIN_COUNTRY_NAME|count|
    //+-----------------+--------------------+-----+
    //|    United States|             Romania|   15|
    //|    United States|              Russia|  161|
    //|       .................................... |
    //|    United States|          Costa Rica|  608|
    //+-----------------+--------------------+-----+
    val flights = spark.read.format("json")
      .load("src/data/flight-data/json/2015-summary.json")
    val just_usa_df = flights.where("dest_country_name = 'United States'")
    just_usa_df.selectExpr("*").show(3,false)
    //+-----------------+-------------------+-----+
    //|DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|
    //+-----------------+-------------------+-----+
    //|United States    |Romania            |15   |
    //|United States    |Croatia            |1    |
    //|United States    |Ireland            |344  |
    //+-----------------+-------------------+-----+
  }

  def dropView(): Unit ={
    spark.sql("SHOW TABLES").show()
    //+--------+-------------------+-----------+
    //|database|          tableName|isTemporary|
    //+--------+-------------------+-----------+
    //| default|      bucketedfiles|      false|
    //| default|            flights|      false|
    //| default|        flights_csv|      false|
    //| default|      flights_empty|      false|
    //| default|flights_from_select|      false|
    //| default|       hive_flights|      false|
    //| default|      just_usa_view|      false|
    //| default|partitioned_flights|      false|
    //+--------+-------------------+-----------+
    spark.sql("""DROP VIEW IF EXISTS just_usa_view""")
    spark.sql("SHOW TABLES").show()
    //+--------+-------------------+-----------+
    //|database|          tableName|isTemporary|
    //+--------+-------------------+-----------+
    //| default|      bucketedfiles|      false|
    //| default|            flights|      false|
    //| default|        flights_csv|      false|
    //| default|      flights_empty|      false|
    //| default|flights_from_select|      false|
    //| default|       hive_flights|      false|
    //| default|partitioned_flights|      false|
    //+--------+-------------------+-----------+
  }

  def showGlobalTemp(): Unit ={
    spark.sql(
      """
        |CREATE GLOBAL TEMP VIEW just_usa_global_view_temp AS
        |SELECT * FROM flights WHERE dest_country_name = 'United States'
        |""".stripMargin)
    spark.catalog.listTables("global_temp").show
    //+--------------------+-----------+-----------+---------+-----------+
    //|                name|   database|description|tableType|isTemporary|
    //+--------------------+-----------+-----------+---------+-----------+
    //|just_usa_global_v...|global_temp|       null|TEMPORARY|       true|
    //+--------------------+-----------+-----------+---------+-----------+
  }

  def databaseHandle(): Unit ={
    spark.sql("DROP DATABASE IF EXISTS some_db")
    spark.sql("CREATE DATABASE some_db")
    spark.sql("""SHOW DATABASES""").show()
    //+---------+
    //|namespace|
    //+---------+
    //|  default|
    //|  some_db|
    //+---------+
    spark.sql("USE some_db")
    spark.sql("SHOW TABLES").show()
    //+--------+---------+-----------+
    //|database|tableName|isTemporary|
    //+--------+---------+-----------+
    //+--------+---------+-----------+
//    spark.sql("SELECT * FROM flights") //fail
    spark.sql("SELECT * FROM default.flights").show()
    //+-----------------+--------------------+-----+
    //|DEST_COUNTRY_NAME| ORIGIN_COUNTRY_NAME|count|
    //+-----------------+--------------------+-----+
    //|    United States|             Romania|   15|
    //|    United States|              Russia|  161|
    //|       .................................... |
    //|    United States|          Costa Rica|  608|
    //+-----------------+--------------------+-----+
    spark.sql("SELECT current_database()").show()
    //+------------------+
    //|current_database()|
    //+------------------+
    //|           some_db|
    //+------------------+
  }

  def selectStatement(): Unit ={
    spark.sql("""DROP TABLE IF EXISTS partitioned_flights""".stripMargin)
    spark.sql("""CREATE TABLE partitioned_flights USING parquet PARTITIONED BY (DEST_COUNTRY_NAME)
                |AS SELECT DEST_COUNTRY_NAME, ORIGIN_COUNTRY_NAME, count FROM flights LIMIT 5""".stripMargin)
    spark.sql("""SELECT
                |CASE WHEN DEST_COUNTRY_NAME = 'UNITED STATES' THEN 1
                |WHEN DEST_COUNTRY_NAME = 'Egypt' THEN 0
                |ELSE -1 END AS case_when
                |FROM partitioned_flights""".stripMargin).show(5,false)
    //+---------+
    //|case_when|
    //+---------+
    //|0        |
    //|-1       |
    //|-1       |
    //|-1       |
    //|-1       |
    //+---------+
  }

  def complexTypeHandle(): Unit ={
    spark.sql("""
                |CREATE VIEW IF NOT EXISTS nested_data AS
                |SELECT (DEST_COUNTRY_NAME, ORIGIN_COUNTRY_NAME)
                |as country, count FROM flights""".stripMargin)
    spark.sql("SELECT * FROM nested_data").show(3,false)
    //+------------------------+-----+
    //|country                 |count|
    //+------------------------+-----+
    //|[United States, Romania]|15   |
    //|[United States, Croatia]|1    |
    //|[United States, Ireland]|344  |
    //+------------------------+-----+
    spark.sql("SELECT country.DEST_COUNTRY_NAME, count FROM nested_data").show(3,false)
    //+-----------------+-----+
    //|DEST_COUNTRY_NAME|count|
    //+-----------------+-----+
    //|United States    |15   |
    //|United States    |1    |
    //|United States    |344  |
    //+-----------------+-----+
    spark.sql("SELECT country.*, count FROM nested_data").show(3,false)
    //+-----------------+-------------------+-----+
    //|DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|
    //+-----------------+-------------------+-----+
    //|United States    |Romania            |15   |
    //|United States    |Croatia            |1    |
    //|United States    |Ireland            |344  |
    //+-----------------+-------------------+-----+
    spark.sql("""SELECT DEST_COUNTRY_NAME as new_name, collect_list(count) as flight_counts,
                |collect_set(ORIGIN_COUNTRY_NAME) as origin_set
                |FROM flights GROUP BY DEST_COUNTRY_NAME""".stripMargin).show(2,false)
    //+--------+-------------+---------------+
    //|new_name|flight_counts|origin_set     |
    //+--------+-------------+---------------+
    //|Anguilla|[41]         |[United States]|
    //|Paraguay|[60]         |[United States]|
    //+--------+-------------+---------------+
    spark.sql("SELECT DEST_COUNTRY_NAME, ARRAY(1, 2, 3) FROM flights").show(2,false)
    //+-----------------+--------------+
    //|DEST_COUNTRY_NAME|array(1, 2, 3)|
    //+-----------------+--------------+
    //|United States    |[1, 2, 3]     |
    //|United States    |[1, 2, 3]     |
    //+-----------------+--------------+
    spark.sql("""SELECT DEST_COUNTRY_NAME as new_name, collect_list(count)[0]
                |FROM flights GROUP BY DEST_COUNTRY_NAME
                |""".stripMargin).show(2,false)
    //+--------+----------------------+
    //|new_name|collect_list(count)[0]|
    //+--------+----------------------+
    //|Anguilla|41                    |
    //|Paraguay|60                    |
    //+--------+----------------------+
    /**
     * You can also do things like convert an array back into rows.
     * You do this by using the explode function.
     * To demonstrate, let’s create a new view as our aggregation:
     */
    spark.sql("""CREATE OR REPLACE TEMP VIEW flights_agg AS
                |SELECT DEST_COUNTRY_NAME, collect_list(count) as collected_counts
                |FROM flights GROUP BY DEST_COUNTRY_NAME""".stripMargin)
    spark.sql("SELECT * FROM flights_agg").show(2,false)
    //+-----------------+----------------+
    //|DEST_COUNTRY_NAME|collected_counts|
    //+-----------------+----------------+
    //|Anguilla         |[41]            |
    //|Paraguay         |[60]            |
    //+-----------------+----------------+
    spark.sql("SELECT explode(collected_counts), DEST_COUNTRY_NAME FROM flights_agg").show(2,false)
    //+---+-----------------+
    //|col|DEST_COUNTRY_NAME|
    //+---+-----------------+
    //|41 |Anguilla         |
    //|60 |Paraguay         |
    //+---+-----------------+
  }

  def functionHandle(): Unit ={
    spark.sql("SHOW FUNCTIONS").show()
    //+--------+
    //|function|
    //+--------+
    //|       !|
    //|      !=|
    //|       %|
    //+--------+
    spark.sql("SHOW SYSTEM FUNCTIONS").show(2)
    //+--------+
    //|function|
    //+--------+
    //|       !|
    //|      !=|
    //+--------+
    /**
     *
    spark.udf.register("power3", power3(_:Double):Double)
    udfExampleDF.selectExpr("power3(num)").show(2)
    udfExampleDF.createOrReplaceTempView("udf_table")
    //持久化
    val sqlS = "CREATE OR REPLACE FUNCTION pow3 AS 'MyUDF' USING JAR 'src/data/udf-1.0-SNAPSHOT.jar'"
    spark.sql(sqlS)
    val exeS = "SELECT pow3(num) AS function_return_value FROM udf_table"
    spark.sql(exeS).show()
     * You can also register functions through the Hive CREATE TEMPORARY FUNCTION syntax.
     */
    spark.sql("SHOW USER FUNCTIONS").show(2)
    //+------------+
    //|    function|
    //+------------+
    //|default.pow3|
    //+------------+
    spark.sql("SHOW FUNCTIONS  's*'").show(2)
    //+--------------+
    //|      function|
    //+--------------+
    //| schema_of_csv|
    //|schema_of_json|
    //+--------------+
    spark.sql("SHOW FUNCTIONS LIKE 'collect*'").show(2)
    //+------------+
    //|    function|
    //+------------+
    //|collect_list|
    //| collect_set|
    //+------------+
  }

  def subQueryHandle(): Unit ={
    spark.sql("""SELECT dest_country_name FROM flights
                |GROUP BY dest_country_name
                |ORDER BY sum(count) DESC
                |LIMIT 5""".stripMargin).show()
    //+-----------------+
    //|dest_country_name|
    //+-----------------+
    //|    United States|
    //|           Canada|
    //|           Mexico|
    //|   United Kingdom|
    //|            Japan|
    //+-----------------+
    /**
     * This query is uncorrelated because it does not include any information from the outer scope of the query.
     * It’s a query that you can run on its own.
     */
    spark.sql("""SELECT * FROM flights
                |WHERE origin_country_name IN (SELECT dest_country_name FROM flights
                |GROUP BY dest_country_name
                |ORDER BY sum(count) DESC
                |LIMIT 5)""".stripMargin).show(5)
    //+-----------------+-------------------+-----+
    //|DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|
    //+-----------------+-------------------+-----+
    //|            Egypt|      United States|   15|
    //|       Costa Rica|      United States|  588|
    //|          Senegal|      United States|   40|
    //|          Moldova|      United States|    1|
    //|           Guyana|      United States|   64|
    //+-----------------+-------------------+-----+
    /**
EXISTS just checks for some existence in the subquery and returns true if there is a value. You
can flip this by placing the NOT operator in front of it. This would be equivalent to finding a flight
to a destination from which you won’t be able to return!
     */
    spark.sql("""SELECT * FROM flights f1
                |WHERE NOT EXISTS (SELECT 1 FROM flights f2
                |WHERE f1.dest_country_name = f2.origin_country_name)
                |AND EXISTS (SELECT 1 FROM flights f2
                |WHERE f2.dest_country_name = f1.origin_country_name)"""
      .stripMargin).show(5)
    //+--------------------+-------------------+-----+
    //|   DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|
    //+--------------------+-------------------+-----+
    //|             Moldova|      United States|    1|
    //|             Algeria|      United States|    4|
    //|Saint Vincent and...|      United States|    1|
    //|        Burkina Faso|      United States|    1|
    //|            Djibouti|      United States|    1|
    //+--------------------+-------------------+-----+
    /**
     * Using uncorrelated scalar queries, you can bring in some supplemental information that you might not have previously.
     * For example, if you wanted to include the maximum value as its own column from the entire counts dataset, you could do this:
     */
    spark.sql("""SELECT *, (SELECT max(count) FROM flights) AS maximum FROM flights""").show(2)
    //+-----------------+-------------------+-----+-------+
    //|DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|maximum|
    //+-----------------+-------------------+-----+-------+
    //|    United States|            Romania|   15| 370002|
    //|    United States|            Croatia|    1| 370002|
    //+-----------------+-------------------+-----+-------+
  }

  def configuration(): Unit ={
    spark.conf.set("spark.sql.shuffle.partitions",30)
    println(spark.conf.get("spark.sql.shuffle.partitions")) //30
    spark.sql("SET spark.sql.shuffle.partitions=20")
    println(spark.conf.get("spark.sql.shuffle.partitions")) //20
  }
}
