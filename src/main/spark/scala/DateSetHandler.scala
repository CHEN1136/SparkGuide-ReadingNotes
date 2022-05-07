package scala

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}


object DateSetHandler {
  /**
   * case class can't put in method
   * @param DEST_COUNTRY_NAME
   * @param ORIGIN_COUNTRY_NAME
   * @param count
   */
  case class FlightMetadata(count: BigInt, randomData: BigInt)
  case class Flight(DEST_COUNTRY_NAME: String,
                    ORIGIN_COUNTRY_NAME: String,
                    count: BigInt)
  val spark = SparkSession.builder().master("local[*]").getOrCreate()
  var flightsDF: DataFrame = null
  var flights: Dataset[Flight] = null

  def main( args:Array[String]): Unit ={
    flightsDF = loadData()
    createDatasets()
    //transformations()
    //joinHandle()
    group_and_aggregation()
  }

  def createDatasets(): Unit ={
    import spark.implicits._
    flights = flightsDF.as[Flight]
    flights.show(5,false)
    //+-----------------+-------------------+-----+
    //|DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|
    //+-----------------+-------------------+-----+
    //|United States    |Romania            |1    |
    //|United States    |Ireland            |264  |
    //|United States    |India              |69   |
    //|Egypt            |United States      |24   |
    //|Equatorial Guinea|United States      |1    |
    //+-----------------+-------------------+-----+
    println(flights.first.DEST_COUNTRY_NAME) //United States
  }

  def loadData(): DataFrame = {
    spark.read.parquet("src/data/flight-data/parquet/2010-summary.parquet/")
  }

  def transformations(): Unit ={
    /**
     * a simple example by creating a simple function that accepts a Flight and returns a Boolean value
     * that describes whether the origin and destination are the same.
     */
    def originIsDestination(flight_row: Flight): Boolean = {
      flight_row.ORIGIN_COUNTRY_NAME == flight_row.DEST_COUNTRY_NAME
    }
    println(flights.filter(flight_row => originIsDestination(flight_row)).first()) //Flight(United States,United States,348113)
    /**
     * we can use it and test it on data on our local machines before using it within Spark
     */
    println(flights.collect().filter(flight_row => originIsDestination(flight_row)).mkString("Array(", ", ", ")")) //Array(Flight(United States,United States,348113))
    /**
     * extract one value from each row
     */
    import spark.implicits._
    val destinations = flights.map(f => f.DEST_COUNTRY_NAME)
    val localDestinations = destinations.take(5)
    localDestinations.foreach(print)
    //United StatesUnited StatesUnited StatesEgyptEquatorial Guinea
  }

  def joinHandle(): Unit ={
    import spark.implicits._
    val flightsMeta = spark.range(500).map(x => (x, scala.util.Random.nextLong))
      .withColumnRenamed("_1", "count").withColumnRenamed("_2", "randomData")
      .as[FlightMetadata]
    flightsMeta.show(2)
    //+-----+--------------------+
    //|count|          randomData|
    //+-----+--------------------+
    //|    0|-6150346972571577543|
    //|    1|-4669615282119679869|
    //+-----+--------------------+
    val flights2 = flights
      .joinWith(flightsMeta, flights.col("count") === flightsMeta.col("count"))
    flights2.show(5,false)
    //+---------------------------------+------------------------+
    //|_1                               |_2                      |
    //+---------------------------------+------------------------+
    //|[United States, Uganda, 1]       |[1, 3267990035442503191]|
    //|[United States, French Guiana, 1]|[1, 3267990035442503191]|
    //|[Bulgaria, United States, 1]     |[1, 3267990035442503191]|
    //|[United States, Slovakia, 1]     |[1, 3267990035442503191]|
    //|[United States, Cameroon, 1]     |[1, 3267990035442503191]|
    //+---------------------------------+------------------------+
    flights2.selectExpr("_1.DEST_COUNTRY_NAME").show(2)
    //+-----------------+
    //|DEST_COUNTRY_NAME|
    //+-----------------+
    //|    United States|
    //|    United States|
    //+-----------------+
    /**
     * random,so the col _2.randomData is changed, use cache to avoid this
     */
    flights2.take(2).foreach(print)
    //(Flight(United States,Uganda,1),FlightMetadata(1,2736093704858406833))
    //(Flight(United States,French Guiana,1),FlightMetadata(1,2736093704858406833))
    var flights3 = flights.join(flightsMeta, Seq("count"))
    flights3.show(2,false)
    //+-----+-----------------+-------------------+-------------------+
    //|count|DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|randomData         |
    //+-----+-----------------+-------------------+-------------------+
    //|1    |United States    |Uganda             |6391289097340552651|
    //|1    |United States    |French Guiana      |6391289097340552651|
    //+-----+-----------------+-------------------+-------------------+
    /**
     * there are no problems joining a DataFrame and a Dataset
     */
    flights3 = flights.join(flightsMeta.toDF(), Seq("count"))
    flights3.show(2,false)
    //+-----+-----------------+-------------------+--------------------+
    //|count|DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|randomData          |
    //+-----+-----------------+-------------------+--------------------+
    //|1    |United States    |Uganda             |-5963343169707404521|
    //|1    |United States    |French Guiana      |-5963343169707404521|
    //+-----+-----------------+-------------------+--------------------+
  }

  def group_and_aggregation(): Unit ={
    import spark.implicits._
    flights.groupBy("DEST_COUNTRY_NAME").count().show()
    flights.groupBy("DEST_COUNTRY_NAME").count().explain()
    /**
 == Physical Plan ==
(2) HashAggregate(keys=[DEST_COUNTRY_NAME#0], functions=[count(1)])
+- Exchange hashpartitioning(DEST_COUNTRY_NAME#0, 200), true, [id=#78]
   +- *(1) HashAggregate(keys=[DEST_COUNTRY_NAME#0], functions=[partial_count(1)])
      +- *(1) ColumnarToRow
         +- FileScan parquet [DEST_COUNTRY_NAME#0] Batched: true, DataFilters: [], Format: Parquet, Location: InMemoryFileIndex[file:/D:/Project/Spark_Project/src/data/flight-data/parquet/2010-summary.parquet], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<DEST_COUNTRY_NAME:string>

     */
    //+--------------------+-----+
    //|   DEST_COUNTRY_NAME|count|
    //+--------------------+-----+
    //|            Anguilla|    1|
    //|              Russia|    1|
    //|            Paraguay|    1|
    //|             Senegal|    1|
    //|              Sweden|    1|
    //+--------------------+-----+
    flights.groupByKey(x => x.ORIGIN_COUNTRY_NAME).count().explain()
    /**== Physical Plan ==
(3) HashAggregate(keys=[value#49], functions=[count(1)])
+- Exchange hashpartitioning(value#49, 200), true, [id=#92]
   +- *(2) HashAggregate(keys=[value#49], functions=[partial_count(1)])
      +- *(2) Project [value#49]
         +- AppendColumns scala.DateSetHandler$$$Lambda$2443/1969632323@5fafa76d, newInstance(class scala.DateSetHandler$Flight), [staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, input[0, java.lang.String, true], true, false) AS value#49]
            +- *(1) ColumnarToRow
               +- FileScan parquet [DEST_COUNTRY_NAME#0,ORIGIN_COUNTRY_NAME#1,count#2L] Batched: t........
     */
    flights.groupByKey(x => x.ORIGIN_COUNTRY_NAME).count().show()
    //+--------------------+--------+
    //|                 key|count(1)|
    //+--------------------+--------+
    //|              Russia|       1|
    //|            Anguilla|       1|
    //|             Senegal|       1|
    //|              Sweden|       1|
    //+--------------------+--------+
    /**
     * After we perform a grouping with a key on a Dataset,
     * we can operate on the Key Value Dataset with functions that will manipulate the groupings as raw objects:
     */
    def grpSum(countryName:String, values: Iterator[Flight]) = {
      values.dropWhile(_.count < 5).map(x => (countryName, x))
    }
    flights.groupByKey(x => x.DEST_COUNTRY_NAME).flatMapGroups(grpSum).show(5,false)
    //+--------+-----------------------------+
    //|_1      |_2                           |
    //+--------+-----------------------------+
    //|Anguilla|[Anguilla, United States, 21]|
    //|Paraguay|[Paraguay, United States, 90]|
    //|Russia  |[Russia, United States, 152] |
    //|Senegal |[Senegal, United States, 29] |
    //|Sweden  |[Sweden, United States, 65]  |
    //+--------+-----------------------------+
    def grpSum2(f:Flight):Integer = {
      1
    }
    flights.groupByKey(x => x.DEST_COUNTRY_NAME).mapValues(grpSum2).count().take(5).foreach(print)
    //(Anguilla,1)(Russia,1)(Paraguay,1)(Senegal,1)(Sweden,1)

    def sum2(left:Flight, right:Flight) = {
      Flight(left.DEST_COUNTRY_NAME, null, left.count + right.count)
  }
    flights.groupByKey(x => x.DEST_COUNTRY_NAME).reduceGroups((l, r) => sum2(l, r))
      .map(x => (x._1, x._2.ORIGIN_COUNTRY_NAME, x._2.count))
      .withColumnRenamed("_1", "DEST_COUNTRY_NAME")
      .withColumnRenamed("_2", "ORIGIN_COUNTRY_NAME")
      .withColumnRenamed("_3", "count")
      .as[Flight]
      .orderBy(col("count").desc)
      .show(3)
    //+-----------------+-------------------+------+
    //|DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME| count|
    //+-----------------+-------------------+------+
    //|    United States|               null|384932|
    //|           Canada|      United States|  8271|
    //|           Mexico|      United States|  6200|
    //+-----------------+-------------------+------+
      //.take(2).foreach(print)
    //(Anguilla,Flight(Anguilla,United States,21))(Russia,Flight(Russia,United States,152)))
  }
}
