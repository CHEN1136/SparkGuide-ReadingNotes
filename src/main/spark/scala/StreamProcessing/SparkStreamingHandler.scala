package scala.StreamProcessing

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Seconds

import scala.util.Random

object SparkStreamingHandler {
  val spark = SparkSession.builder().master("local[*]").getOrCreate()
  val static = spark.read.json("src/data/activity-data/")
  val dataSchema = static.schema
  val streaming = spark.readStream.schema(dataSchema)
    .option("maxFilesPerTrigger", 1).json("src/data/activity-data")
  spark.conf.set("spark.sql.shuffle.partitions", 5)
  case class Flight(DEST_COUNTRY_NAME: String, ORIGIN_COUNTRY_NAME: String,
                    count: BigInt)

  def main(args: Array[String]): Unit ={
    //dataShow()
    //countHandle()
    //select_and_filter()
    //joinHandle()
    //triggerHandle()
    datasetHandleInStreaming()
  }

  def transformationsHandle(): Unit ={

  }

  def countHandle(): Unit ={
    val activityCounts = streaming.groupBy("gt").count()
    val activityQuery = activityCounts.writeStream.queryName("activity_counts")
      .format("memory").outputMode("complete")
      .start()
    for( i <- 1 to 5 ) {
      spark.sql("SELECT * FROM activity_counts").show()
      Thread.sleep(1000)
    }
    //+----------+-----+
    //|        gt|count|
    //+----------+-----+
    //|       sit|12309|
    //|     stand|11384|
    //|stairsdown| 9365|
    //|      walk|13256|
    //|  stairsup|10452|
    //|      null|10449|
    //|      bike|10796|
    //+----------+-----+
    //+----------+-----+
    //|       sit|24619|
    //|     stand|22769|
    //|stairsdown|18729|
    //|      walk|26512|
    //|  stairsup|20905|
    //|      null|20896|
    //|      bike|21593|
    //+----------+-----+
    activityQuery.awaitTermination()
  }

  def dataShow(): Unit ={
    static.persist(StorageLevel.MEMORY_AND_DISK)
    static.show(4,false)
    //+-------------+-------------------+--------+-----+------+----+-----+------------+------------+------------+
    //| Arrival_Time|      Creation_Time|  Device|Index| Model|User|   gt|           x|           y|           z|
    //+-------------+-------------------+--------+-----+------+----+-----+------------+------------+------------+
    //|1424686735090|1424686733090638193|nexus4_1|   18|nexus4|   g|stand| 3.356934E-4|-5.645752E-4|-0.018814087|
    //|1424686735292|1424688581345918092|nexus4_2|   66|nexus4|   g|stand|-0.005722046| 0.029083252| 0.005569458|
    //|1424686735500|1424686733498505625|nexus4_1|   99|nexus4|   g|stand|   0.0078125|-0.017654419| 0.010025024|
    //|1424686735691|1424688581745026978|nexus4_2|  145|nexus4|   g|stand|-3.814697E-4|   0.0184021|-0.013656616|
    //+-------------+-------------------+--------+-----+------+----+-----+------------+------------+------------+
    println(dataSchema)
    //root
    //|-- Arrival_Time: long (nullable = true)
    //|-- Creation_Time: long (nullable = true)
    //|-- Device: string (nullable = true)
    //|-- Index: long (nullable = true)
    //|-- Model: string (nullable = true)
    //|-- User: string (nullable = true)
    //|-- _corrupt_record: string (nullable = true)
    //|-- gt: string (nullable = true)
    //|-- x: double (nullable = true)
    //|-- y: double (nullable = true)
    //|-- z: double (nullable = true)
  }

  def select_and_filter(): Unit ={
    import org.apache.spark.sql.functions.expr
    val simpleTransform = streaming.withColumn("stairs", expr("gt like '%stairs%'"))
      .where("stairs")
      .where("gt is not null")
      .select("gt", "model", "arrival_time", "creation_time")
      .writeStream
      .queryName("simple_transform")
      .format("memory")
      .outputMode("append")
      .start()

    for( i <- 1 to 5 ) {
      spark.sql("SELECT * FROM simple_transform").show()
      //    //+--------+------+-------------+-------------------+
      //    //|      gt| model| arrival_time|      creation_time|
      //    //+--------+------+-------------+-------------------+
      //    //|stairsup|nexus4|1424687983719|1424687981726802718|
      //    //|stairsup|nexus4|1424687984000|1424687982009853255|
      //    //|stairsup|nexus4|1424687984404|1424687982411977009|
      //    //|stairsup|nexus4|1424687984805|1424687982814351277|
      Thread.sleep(1000)
    }

    simpleTransform.awaitTermination()
  }

  def aggregationsHandle(): Unit ={
    val deviceModelStats = streaming.cube("gt", "model").avg()
      .drop("avg(Arrival_time)")
      .drop("avg(Creation_Time)")
      .drop("avg(Index)")
      .writeStream.queryName("device_counts").format("memory").outputMode("complete")
      .start()
    for (i <- 1 to 5) {
      spark.sql("SELECT * FROM device_counts").show()
      Thread.sleep(1000)
    }
    //+----------+------+--------------------+--------------------+--------------------+
    //|        gt| model|              avg(x)|              avg(y)|              avg(z)|
    //+----------+------+--------------------+--------------------+--------------------+
    //|       sit|  null|-4.84874225480320...|3.237740430277438...|-4.65213713798297E-5|
    //|     stand|  null|-3.19865608911239...|4.070066943871052E-4|1.027885098598942...|
    //|       sit|nexus4|-4.84874225480320...|3.237740430277438...|-4.65213713798297E-5|
    //|     stand|nexus4|-3.19865608911239...|4.070066943871052E-4|1.027885098598942...|
    //|      null|  null|-0.00601179783148...|-7.22352747272202...|0.003847526633724...|
    deviceModelStats.awaitTermination()
  }

  def joinHandle(): Unit ={
    val historicalAgg = static.groupBy("gt", "model").avg()
    val deviceModelStats = streaming.drop("Arrival_Time", "Creation_Time", "Index")
      .cube("gt", "model").avg()
      .withColumnRenamed("gt", "newGt")
      .join(historicalAgg, col("newGt") === historicalAgg("gt"))
      .writeStream.queryName("device_counts").format("memory").outputMode("complete")
      .start()
    for (i <- 1 to 5) {
      spark.sql("SELECT * FROM device_counts").show(3)
      Thread.sleep(1000)
    }
    //+----------+------+--------------------+--------------------+--------------------+--------------------+--------------------+------------------+--------------------+--------------------+--------------------+
    //|        gt| model|              avg(x)|              avg(y)|              avg(z)|   avg(Arrival_Time)|  avg(Creation_Time)|        avg(Index)|              avg(x)|              avg(y)|              avg(z)|
    //+----------+------+--------------------+--------------------+--------------------+--------------------+--------------------+------------------+--------------------+--------------------+--------------------+
    //|      bike|nexus4|0.023512544470933663|-0.01304747996973...|-0.08360475809007027|1.424751133888726E12|1.424752127164280...|326459.54404620576|0.023370413164907837|-0.00957632248813...|-0.08231609355023549|
    //|      null|nexus4|-0.00302501221506...|-0.00410754501410...|0.005961452067049477|1.424749007889033...|1.424749925564171...| 219259.7088862013|-0.00744121740269...|-4.14276537167517...|0.004478045213719586|
    //|stairsdown|nexus4|0.028103791071500104|-0.03570080351911373| 0.12203047970606548|1.424744595170351...|1.424745504009643...|230452.54472381322|0.025192660067633148|-0.04060181003955051| 0.12679137080213765|
    //+----------+------+--------------------+--------------------+--------------------+--------------------+--------------------+------------------+--------------------+--------------------+--------------------+
    deviceModelStats.awaitTermination()
  }

  def triggerHandle(): Unit ={
    //triggerByTime()
    triggerOnce()
  }

  def triggerOnce(): Unit ={
    val activityCounts = streaming.groupBy("gt").count()
    val timeTrigger = activityCounts.writeStream.trigger(Trigger.Once())
      .format("console")
      .outputMode("complete")
      .foreachBatch{
        (batchDF: DataFrame, batchId: Long) => {
          batchDF.withColumn("CurrentTime", lit(System.currentTimeMillis())).withColumn("batchId", lit(batchId))
              .write.csv(s"tmp/triggerOnce/_${Random.nextInt()}.csv")
        }
      }.start()
    timeTrigger.awaitTermination()
  }

  def triggerByTime(): Unit ={
    val activityCounts = streaming.groupBy("gt").count()
    val timeTrigger = activityCounts.writeStream.trigger(Trigger.ProcessingTime("10 seconds"))
      .format("console")
      .outputMode("complete")
      .foreachBatch{
        (batchDF: DataFrame, batchId: Long) => {
          batchDF.withColumn("CurrentTime", lit(System.currentTimeMillis())).withColumn("batchId", lit(batchId))
            .write.csv(s"tmp/triggerByTime_${Random.nextInt()}.csv")
        }
      }.start()
    timeTrigger.awaitTermination()
  }

  def datasetHandleInStreaming(): Unit ={
    import spark.implicits._
    //spark.sparkContext.setLogLevel("INFO")
    val dataSchema = spark.read
      .parquet("src/data/flight-data/parquet/2010-summary.parquet/")
      .schema
    val flightsDF = spark.readStream.schema(dataSchema)
      .parquet("src/data/flight-data/parquet/*/")
    val flights = flightsDF.as[Flight]
    def originIsDestination(flight_row: Flight): Boolean = {
      return flight_row.ORIGIN_COUNTRY_NAME == flight_row.DEST_COUNTRY_NAME
    }
    val dsh = flights.filter(flight_row => originIsDestination(flight_row))
      .groupByKey(x => x.DEST_COUNTRY_NAME).count()
      .writeStream.queryName("device_counts").format("console").outputMode("complete")
      .start()
    //-------------------------------------------
    //Batch: 0
    //-------------------------------------------
    //+-------------+--------+
    //|          key|count(1)|
    //+-------------+--------+
    //|United States|       1|
    //+-------------+--------+
    dsh.awaitTermination()
  }


}
