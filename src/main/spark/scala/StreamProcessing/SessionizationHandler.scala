package scala.StreamProcessing

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{GroupStateTimeout, OutputMode, StreamingQueryListener}

object SessionizationHandler {
  val spark = SparkSession.builder().master("local[*]").getOrCreate()
  spark.conf.set("spark.sql.shuffle.partitions", 5)
  val static = spark.read.json("src/data/activity-data-for-stateful")
  val streaming = spark
    .readStream
    .schema(static.schema)
    .option("maxFilesPerTrigger", 10)
    .json("src/data/activity-data-for-stateful")

  case class InputRow(uid: String, timestamp: java.sql.Timestamp, x: Double,
                      activity: String)

  case class UserSession(val uid: String, var timestamp: java.sql.Timestamp,
                         var activities: Array[String], var values: Array[Double])

  case class UserSessionOutput(val uid: String, var activities: Array[String],
                               var xAvg: Double)


  def main(args: Array[String]): Unit = {
    import spark.implicits._
    spark.streams.addListener(new StreamingQueryListener {
      override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
        println("query started:" + event.id)
        //query started:705a1cb9-0099-4571-8d49-c7b358edb063
      }

      override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
        println("Query made progress:" + event.progress.json)
        //Query made progress:{"id":"705a1cb9-0099-4571-8d49-c7b358edb063","runId":"f3784e27-d2ad-457f-857f-be95bb8959d5","name":"count_based_device","timestamp":"2022-05-14T06:04:57.713Z","batchId":0,"numInputRows":23,"processedRowsPerSecond":9.721048182586644,"durationMs":{"addBatch":1356,"getBatch":51,"latestOffset":257,"queryPlanning":217,"triggerExecution":2365,"walCommit":228},"eventTime":{"avg":"2015-02-23T09:59:19.480Z","max":"2015-02-25T23:25:44.591Z","min":"2015-02-20T04:32:24.591Z","watermark":"1970-01-01T00:00:00.000Z"},"stateOperators":[{"numRowsTotal":2,"numRowsUpdated":2,"memoryUsedBytes":1880,"customMetrics":{"loadedMapCacheHitCount":0,"loadedMapCacheMissCount":0,"stateOnCurrentVersionSizeBytes":1160}}],"sources":[{"description":"FileStreamSource[file:/D:/Project/Spark_Project/src/data/activity-data-for-stateful]","startOffset":null,"endOffset":{"logOffset":0},"numInputRows":23,"processedRowsPerSecond":9.721048182586644}],"sink":{"description":"MemorySink","numOutputRows":4}}
        //Query made progress:{"id":"705a1cb9-0099-4571-8d49-c7b358edb063","runId":"f3784e27-d2ad-457f-857f-be95bb8959d5","name":"count_based_device","timestamp":"2022-05-14T06:05:00.096Z","batchId":1,"numInputRows":0,"inputRowsPerSecond":0.0,"processedRowsPerSecond":0.0,"durationMs":{"addBatch":570,"getBatch":1,"latestOffset":4,"queryPlanning":80,"triggerExecution":1078,"walCommit":222},"eventTime":{"watermark":"2015-02-25T23:25:39.591Z"},"stateOperators":[{"numRowsTotal":2,"numRowsUpdated":1,"memoryUsedBytes":2968,"customMetrics":{"loadedMapCacheHitCount":5,"loadedMapCacheMissCount":0,"stateOnCurrentVersionSizeBytes":1120}}],"sources":[{"description":"FileStreamSource[file:/D:/Project/Spark_Project/src/data/activity-data-for-stateful]","startOffset":{"logOffset":0},"endOffset":{"logOffset":0},"numInputRows":0,"inputRowsPerSecond":0.0,"processedRowsPerSecond":0.0}],"sink":{"description":"MemorySink","numOutputRows":0}}
        //Query made progress:{"id":"705a1cb9-0099-4571-8d49-c7b358edb063","runId":"f3784e27-d2ad-457f-857f-be95bb8959d5","name":"count_based_device","timestamp":"2022-05-14T06:05:01.175Z","batchId":2,"numInputRows":0,"inputRowsPerSecond":0.0,"processedRowsPerSecond":0.0,"durationMs":{"latestOffset":3,"triggerExecution":4},"eventTime":{"watermark":"2015-02-25T23:25:39.591Z"},"stateOperators":[{"numRowsTotal":2,"numRowsUpdated":0,"memoryUsedBytes":2968,"customMetrics":{"loadedMapCacheHitCount":5,"loadedMapCacheMissCount":0,"stateOnCurrentVersionSizeBytes":1120}}],"sources":[{"description":"FileStreamSource[file:/D:/Project/Spark_Project/src/data/activity-data-for-stateful]","startOffset":{"logOffset":0},"endOffset":{"logOffset":0},"numInputRows":0,"inputRowsPerSecond":0.0,"processedRowsPerSecond":0.0}],"sink":{"description":"MemorySink","numOutputRows":0}}
      }

      override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit =
        println("Query terminated:" + event.id)
    })

    val withEventTime = streaming.selectExpr(
      "*",
      "cast(cast(Creation_Time as double)/1000000000 as timestamp) as event_time")

    val stream = withEventTime.where("x is not null")
      .selectExpr("user as uid",
        "cast(Creation_Time/1000000000 as timestamp) as timestamp",
        "x", "gt as activity")
      .as[InputRow]
      .withWatermark("timestamp", "5 minutes")
      .groupByKey(_.uid)
      .flatMapGroupsWithState(OutputMode.Append,
        GroupStateTimeout.EventTimeTimeout)(updateAcrossEvents)
      .writeStream
      .queryName("count_based_device")
      .format("memory")
      .start()

    for (i <- 1 to 3) {
      spark.sql("SELECT * FROM count_based_device").show(false)
      //println(stream.recentProgress.mkString("Array(", ", ", ")"))
      Thread.sleep(10000)
    }
    //+---+-----------------------------+----+
    //|uid|activities                   |xAvg|
    //+---+-----------------------------+----+
    //|a  |[bike, stand, walk]          |2.6 |
    //|b  |[swim, stand, walk, run]     |2.6 |
    //|b  |[sit, stand, bike, run, walk]|2.4 |
    //+---+-----------------------------+----+
    stream.awaitTermination()
  }


  /**
   * update the individual state based on a single input row
   */
  def updateWithEvent(state: UserSession, input: InputRow): UserSession = {
    // handle malformed dates
    if (Option(input.timestamp).isEmpty) {
      return state
    }
    state.timestamp = input.timestamp
    state.values = state.values ++ Array(input.x)
    if (!state.activities.contains(input.activity)) {
      state.activities = state.activities ++ Array(input.activity)
    }
    state
  }


  import org.apache.spark.sql.streaming.{
    GroupStateTimeout, OutputMode,
    GroupState
  }

  def updateAcrossEvents(uid: String,
                         inputs: Iterator[InputRow],
                         oldState: GroupState[UserSession]): Iterator[UserSessionOutput] = {
    inputs.toSeq.sortBy(_.timestamp.getTime).toIterator.flatMap { input =>
      val state = if (oldState.exists) oldState.get else UserSession(
        uid,
        new java.sql.Timestamp(6284160000000L),
        Array(),
        Array())
      val newState = updateWithEvent(state, input)

      println(input)
      statePrint(newState)
      println(oldState.getCurrentWatermarkMs())
      println()

      if (oldState.hasTimedOut) {
        println("this is timeout handler")
        val state = oldState.get
        oldState.remove()
        Iterator(UserSessionOutput(uid,
          state.activities,
          newState.values.sum / newState.values.length.toDouble))
      } else if (state.values.length > 4) {
        val state = oldState.get
        oldState.remove()
        Iterator(UserSessionOutput(uid,
          state.activities,
          newState.values.sum / newState.values.length.toDouble))
      } else {
        oldState.update(newState)
        //println(newState.timestamp.getTime - 1000000000L)
        //Timeout timestamp (1423906744591) cannot be earlier than the current watermark (1424688284880)
        oldState.setTimeoutTimestamp(newState.timestamp.getTime, "5 minutes")
        Iterator()
      }
    }
  }

  def statePrint(state : UserSession): Unit ={
    var s = ""
    s += "uid:" + state.uid + " "
    s += "value:["
    for (i <- state.values)
      s += i.toString + ","
    s += "] "
    s += "timestamp:" + state.timestamp.toString + " "
    s += "activity:["
    for( i <- state.activities) {
      s += i.toString + ","
    }
    s += "]"
    println(s)
  }
}
