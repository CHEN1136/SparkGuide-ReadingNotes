package scala.StreamProcessing

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{GroupStateTimeout, OutputMode}

import scala.StreamProcessing.EventTimeHandler.{spark, streaming}

object arbitraryStatefulProcessing {
  val spark = SparkSession.builder().master("local[*]").getOrCreate()
  spark.conf.set("spark.sql.shuffle.partitions", 5)
  val static = spark.read.json("src/data/activity-data")
  val streaming = spark
    .readStream
    .schema(static.schema)
    .option("maxFilesPerTrigger", 10)
    .json("src/data/activity-data")

  case class InputRow(device: String, timestamp: java.sql.Timestamp, x: Double)
  case class DeviceState(device: String, var values: Array[Double],
                         var count: Int)
  case class OutputRow(device: String, previousAverage: Double)

  def main(args: Array[String]): Unit = {
    avg_per_500()
  }

  def avg_per_500(): Unit ={
    import spark.implicits._
    val withEventTime = streaming.selectExpr(
      "*",
      "cast(cast(Creation_Time as double)/1000000000 as timestamp) as event_time")
    val stream = withEventTime
      .selectExpr("Device as device",
        "cast(Creation_Time/1000000000 as timestamp) as timestamp", "x")
      .as[InputRow]
      .groupByKey(_.device)
      .flatMapGroupsWithState(OutputMode.Append,
        GroupStateTimeout.NoTimeout)(updateAcrossEvents)
      .writeStream
      .queryName("count_based_device")
      .format("memory")
      .outputMode("append")
      .start()
    for( i <- 1 to 3) {
      spark.sql("SELECT * FROM count_based_device").show(3)
      Thread.sleep(30000)
    }
    //+--------+--------------------+
    //|  device|     previousAverage|
    //+--------+--------------------+
    //|nexus4_1|8.052062900000007E-4|
    //|nexus4_1|-5.61523414000000...|
    //|nexus4_1|-1.27868660999999...|
    //+--------+--------------------+
    stream.awaitTermination()
  }

  /**
   * update the individual state based on a single input row
   */
  def updateWithEvent(state:DeviceState, input:InputRow):DeviceState = {
    state.count += 1
    // maintain an array of the x-axis values, like union
    state.values = state.values ++ Array(input.x)
    state
  }


  import org.apache.spark.sql.streaming.{GroupStateTimeout, OutputMode,
    GroupState}
  def updateAcrossEvents(device:String, inputs: Iterator[InputRow],
                         oldState: GroupState[DeviceState]):Iterator[OutputRow] = {
    inputs.toSeq.sortBy(_.timestamp.getTime).toIterator.flatMap { input =>
      val state = if (oldState.exists) oldState.get
      else DeviceState(device, Array(), 0)
      val newState = updateWithEvent(state, input)
      if (newState.count >= 30000) {
        // One of our windows is complete; replace our state with an empty
        // DeviceState and output the average for the past 500 items from
        // the old state
        oldState.update(DeviceState(device, Array(), 0))
        Iterator(OutputRow(device,
          newState.values.sum / newState.values.length.toDouble))
      }
      else {
        // Update the current DeviceState object in place and output no
        // records
        oldState.update(newState)
        Iterator()
      }
    }
  }


}
