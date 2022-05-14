package scala.StreamProcessing

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{GroupStateTimeout, Trigger}

object EventTimeHandler {
  val spark = SparkSession.builder().master("local[*]").getOrCreate()
  spark.conf.set("spark.sql.shuffle.partitions", 5)
  case class InputRow(user:String, timestamp:java.sql.Timestamp, activity:String)
  case class UserState(user:String,
                       var activity:String,
                       var start:java.sql.Timestamp,
                       var end:java.sql.Timestamp)

  val static = spark.read.json("src/data/activity-data")
  val streaming = spark
    .readStream
    .schema(static.schema)
    .option("maxFilesPerTrigger", 10)
    .json("src/data/activity-data")
  //static.printSchema()
  //root
  // |-- Arrival_Time: long (nullable = true)
  // |-- Creation_Time: long (nullable = true)
  // |-- Device: string (nullable = true)
  // |-- Index: long (nullable = true)
  // |-- Model: string (nullable = true)
  // |-- User: string (nullable = true)
  // |-- gt: string (nullable = true)
  // |-- x: double (nullable = true)
  // |-- y: double (nullable = true)
  // |-- z: double (nullable = true)

  def main(args: Array[String]): Unit = {
    //windowsHandle()
    //watermarkHandle()
    //duplicateHandle()
    arbitraryStateHandle()
  }

  def windowsHandle(): Unit ={
    val withEventTime = streaming.selectExpr(
      "*",
      "cast(cast(Creation_Time as double)/1000000000 as timestamp) as event_time")
    static.selectExpr(  "Creation_Time",
      "cast(cast(Creation_Time as double)/1000000000 as timestamp) as event_time").show(2,false)
    //+-------------------+--------------------------+
    //|Creation_Time      |event_time                |
    //+-------------------+--------------------------+
    //|1424686728649478625|2015-02-23 18:18:48.649478|
    //|1424688581255250367|2015-02-23 18:49:41.25525 |
    //+-------------------+--------------------------+
    import org.apache.spark.sql.functions.{window, col}
//    val stream = withEventTime.groupBy(window(col("event_time"), "10 minutes")).count()
//      .writeStream
//      .queryName("events_per_window")
//      .format("memory")
//      .outputMode("complete")
//      .start()
    //+------------------------------------------+-----+
    //|window                                    |count|
    //+------------------------------------------+-----+
    //|[2015-02-23 18:40:00, 2015-02-23 18:50:00]|11035|
    //|[2015-02-24 19:50:00, 2015-02-24 20:00:00]|18854|
    //|[2015-02-24 21:00:00, 2015-02-24 21:10:00]|16636|
    //|[2015-02-23 21:20:00, 2015-02-23 21:30:00]|13269|
    //|[2015-02-22 08:40:00, 2015-02-22 08:50:00]|5    |
    //+------------------------------------------+-----+
//    val stream = withEventTime.groupBy(window(col("event_time"), "10 minutes"),col("User")).count()
//      .writeStream
//      .queryName("events_per_window")
//      .format("memory")
//      .outputMode("complete")
//      .start()

    val stream = withEventTime.groupBy(window(col("event_time"), "10 minutes", "5 minutes"))
      .count()
      .writeStream
      .queryName("events_per_window")
      .format("memory")
      .outputMode("complete")
      .start()
    //+------------------------------------------+-----+
    //|window                                    |count|
    //+------------------------------------------+-----+
    //|[2015-02-23 22:15:00, 2015-02-23 22:25:00]|13523|
    //|[2015-02-24 19:50:00, 2015-02-24 20:00:00]|18854|
    //|[2015-02-24 21:00:00, 2015-02-24 21:10:00]|16636|
    //|[2015-02-22 08:35:00, 2015-02-22 08:45:00]|5    |
    //|[2015-02-23 20:30:00, 2015-02-23 20:40:00]|12617|
    //+------------------------------------------+-----+


    for( i <- 1 to 3) {
//      spark.sql("SELECT window.start, window.end, user, count FROM events_per_window").show(5,false)
      //+-------------------+-------------------+----+-----+
      //|start              |end                |user|count|
      //+-------------------+-------------------+----+-----+
      //|2015-02-23 18:20:00|2015-02-23 18:30:00|g   |12403|
      //|2015-02-24 21:30:00|2015-02-24 21:40:00|b   |10175|
      //|2015-02-24 20:20:00|2015-02-24 20:30:00|f   |16712|
      //|2015-02-24 23:00:00|2015-02-24 23:10:00|e   |12468|
      //|2015-02-24 21:00:00|2015-02-24 21:10:00|f   |4146 |
      //+-------------------+-------------------+----+-----+
      spark.sql("SELECT * FROM events_per_window").show(5,false)
      Thread.sleep(4000)
    }
    //root
    //|-- window: struct (nullable = false)
    //| |-- start: timestamp (nullable = true)
    //| |-- end: timestamp (nullable = true)
    //|-- count: long (nullable = false)

    stream.awaitTermination()
  }

  def watermarkHandle(): Unit ={
    import org.apache.spark.sql.functions.{window, col}
    val myStatic = spark.read.json("src/data/myData")
    //
    val myDF = spark
      .readStream
      .schema(myStatic.schema)
      .option("maxFilesPerTrigger", 10)
      .json("src/data/myData")
    val withEventTime = myDF.selectExpr(
      "*",
      "cast(Creation_Time as timestamp) as event_time")

    val stream = withEventTime
      .withWatermark("event_time", "10 minutes")
      .groupBy(window(col("event_time"), "10 minutes", "5 minutes"),col("User"))
      .count()
      .writeStream
      .queryName("events_per_window")
      .format("memory")
      .outputMode("append")
      .start()
    for( i <- 1 to 5){
      spark.sql("select * from events_per_window order by window.start").show(false)
      //+------------------------------------------+----+-----+
      //|window                                    |User|count|
      //+------------------------------------------+----+-----+
      //|[2022-05-12 14:25:00, 2022-05-12 14:35:00]|dog |1    |
      //|[2022-05-12 14:25:00, 2022-05-12 14:35:00]|cat |1    |
      //|[2022-05-12 14:25:00, 2022-05-12 14:35:00]|owl |1    |
      //|[2022-05-12 14:30:00, 2022-05-12 14:40:00]|cat |1    |
      //|[2022-05-12 14:30:00, 2022-05-12 14:40:00]|owl |1    |
      //|[2022-05-12 14:30:00, 2022-05-12 14:40:00]|dog |1    |
      //|[2022-05-12 14:40:00, 2022-05-12 14:50:00]|owl |1    |
      //|[2022-05-12 14:40:00, 2022-05-12 14:50:00]|cat |2    |
      //|[2022-05-12 14:40:00, 2022-05-12 14:50:00]|dog |1    |
      //+------------------------------------------+----+-----+
      Thread.sleep(4000)
    }
    stream.awaitTermination()
  }

  def duplicateHandle(): Unit ={
    import org.apache.spark.sql.functions.expr
    val withEventTime = streaming.selectExpr(
      "*",
      "cast(cast(Creation_Time as double)/1000000000 as timestamp) as event_time")
    val streamDeduplicate = withEventTime
      .withWatermark("event_time", "5 seconds")
      .dropDuplicates("User", "event_time")
      .groupBy("User")
      .count()
      .writeStream
      .queryName("deduplicated")
      .format("memory")
      .outputMode("complete")
      .start()

    val stream = withEventTime
      .withWatermark("event_time", "5 seconds")
      .groupBy("User")
      .count()
      .writeStream
      .queryName("common")
      .format("memory")
      .outputMode("complete")
      .start()

    for(i <- 1 to 3) {
      spark.sql("select * from deduplicated").show(3)
      //+----+-----+
      //|User|count|
      //+----+-----+
      //|   a|80850|
      //|   b|91230|
      //|   c|77150|
      //+----+-----+
      spark.sql("select * from common").show(3)
      //+----+------+
      //|User| count|
      //+----+------+
      //|   a| 88935|
      //|   b|100353|
      //|   c| 84865|
      //+----+------+
      Thread.sleep(4000)
    }
    streamDeduplicate.awaitTermination()
    stream.awaitTermination()
  }

  def arbitraryStateHandle(): Unit ={
    import spark.implicits._
    val withEventTime = streaming.selectExpr(
      "*",
      "cast(cast(Creation_Time as double)/1000000000 as timestamp) as event_time")
    val stream = withEventTime
      .selectExpr("User as user",
        "cast(Creation_Time/1000000000 as timestamp) as timestamp", "gt as activity")
      .as[InputRow]
      .groupByKey(_.user)
      .mapGroupsWithState(GroupStateTimeout.NoTimeout())(updateAcrossEvents)
      .writeStream
      .queryName("events_per_window")
      .format("memory")
      .outputMode("update")
      .start()
    for(i <- 1 to 6){
      spark.sql("SELECT * FROM events_per_window order by user, start").show(3,false)
      //+----+--------+--------------------------+--------------------------+
      //|user|activity|start                     |end                       |
      //+----+--------+--------------------------+--------------------------+
      //|a   |bike    |2015-02-23 21:30:15.737244|2015-02-23 22:06:02.817334|
      //|b   |bike    |2015-02-24 22:01:44.044777|2015-02-24 22:38:48.106599|
      //|c   |bike    |2015-02-23 20:40:27.656625|2015-02-23 21:15:55.059924|
      //+----+--------+--------------------------+--------------------------+
      Thread.sleep(4000)
    }

    stream.awaitTermination()
  }

  def updateUserStateWithEvent(state:UserState, input:InputRow):UserState = {
    if (Option(input.timestamp).isEmpty) {
      return state
    }
    if (state.activity == input.activity) {
      if (input.timestamp.after(state.end)) {
        state.end = input.timestamp
      }
      if (input.timestamp.before(state.start)) {
        state.start = input.timestamp
      }
    } else {
      if (input.timestamp.after(state.end)) {
        state.start = input.timestamp
        state.end = input.timestamp
        state.activity = input.activity
      }
    }
    state
  }

  import org.apache.spark.sql.streaming.{GroupStateTimeout, OutputMode, GroupState}
  def updateAcrossEvents(user:String,
                         inputs: Iterator[InputRow],
                         oldState: GroupState[UserState]):UserState = {
    var state:UserState = if (oldState.exists) oldState.get else UserState(user,
      "",
      new java.sql.Timestamp(6284160000000L),
      new java.sql.Timestamp(6284160L)
    )
    // we simply specify an old date that we can compare against and
    // immediately update based on the values in our data
    for (input <- inputs) {
      state = updateUserStateWithEvent(state, input)
      oldState.update(state)
    }
    state
  }


}
