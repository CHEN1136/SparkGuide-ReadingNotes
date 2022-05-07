package scala.DistributedSharedVariables

import org.apache.spark.sql.SparkSession
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}

object DSVHandler {
  val spark = SparkSession.builder().master("local[*]").getOrCreate()
  import spark.implicits._
  case class Flight(DEST_COUNTRY_NAME: String,
                    ORIGIN_COUNTRY_NAME: String, count: BigInt)
  var sc = spark.sparkContext

  def main(args: Array[String]): Unit ={
    //broadcastVariablesHandle()
    //accumulatorHandle()
    customAccumulatorHandle()
  }

  def broadcastVariablesHandle(): Unit ={
    val myCollection = "Spark The Definitive Guide : Big Data Processing Made Simple"
      .split(" ")
    val words = spark.sparkContext.parallelize(myCollection, 2)

    val supplementalData = Map("Spark" -> 1000, "Definitive" -> 200,
      "Big" -> -300, "Simple" -> 100)

    /**
     * We can broadcast this structure across Spark and reference it by using suppBroadcast
     * This value is immutable and is lazily replicated across all nodes in the cluster when we trigger an action
     */
    val suppBroadcast = spark.sparkContext.broadcast(supplementalData)

    suppBroadcast.value.foreach(println)
    //(Spark,1000)
    //(Definitive,200)
    //(Big,-300)
    //(Simple,100)
    println(suppBroadcast.value.getClass) //class scala.collection.immutable.Map$Map4

    words.map(word => (word, suppBroadcast.value.getOrElse(word, 0)))
      .sortBy(wordPair => wordPair._2)
      .collect().foreach(println)
    //(Big,-300)
    //(The,0)
    //(Guide,0)
    //(:,0)
    //(Data,0)
    //(Processing,0)
    //(Made,0)
    //(Simple,100)
    //(Definitive,200)
    //(Spark,1000)

  }

  def accumulatorHandle(): Unit ={
    val flights = spark.read
      .parquet("src/data/flight-data/parquet/2010-summary.parquet")
      .as[Flight]

    /**
     *  count the number of flights to or from China
     */
    import org.apache.spark.util.LongAccumulator
    val accUnnamed = new LongAccumulator
    val acc = spark.sparkContext.register(accUnnamed)

    /**
     * instantiate the accumulator and register it with a name
     */
    val accChina = new LongAccumulator
    val accChina2 = spark.sparkContext.longAccumulator("China")
    spark.sparkContext.register(accChina, "China")
    //Named accumulators will display in the Spark UI, whereas unnamed ones will not.
    def accChinaFunc(flight_row: Flight): Unit = {
      val destination = flight_row.DEST_COUNTRY_NAME
      val origin = flight_row.ORIGIN_COUNTRY_NAME
      if (destination == "China") {
        accChina.add(flight_row.count.toLong)
      }
      else if (origin == "China") {
        accChina.add(flight_row.count.toLong)
      }
    }

    /**
     * iterate over every row in our flights dataset via the foreach method
     *  foreach is an action
     *  Spark can provide guarantees that perform only inside of actions
     */
    flights.foreach(flight_row => accChinaFunc(flight_row))

    println(accChina.value) //953
  }

  def customAccumulatorHandle(): Unit ={
    val flights = spark.read
      .parquet("src/data/flight-data/parquet/2010-summary.parquet")
      .as[Flight]
    import scala.collection.mutable.ArrayBuffer
    import org.apache.spark.util.AccumulatorV2
    val arr = ArrayBuffer[BigInt]()
    val acc = new EvenAccumulator
    sc.register(acc, "evenAcc")
    // in Scala
    println(acc.value) // 0
    flights.foreach(flight_row => acc.add(flight_row.count))
    println(acc.value) // 31390
    println(acc.count) //122
    println(acc.avg) //257.2950819672131
  }

}
class EvenAccumulator extends AccumulatorV2[BigInt, BigInt] {
  private var num:BigInt = 0
  private var _count:BigInt = 0
  def sum : BigInt = num
  def count : BigInt = _count
  def avg : Double = num.toDouble / _count.toLong
  def reset(): Unit = {
    this.num = 0
    this._count = 0
  }
  def add(intValue: BigInt): Unit = {
    if (intValue % 2 == 0) {
      this.num += intValue
      this._count += 1
    }
  }
  def merge(other: AccumulatorV2[BigInt,BigInt]): Unit = other match{
    case o: EvenAccumulator =>
      num += o.value()
    _count += o.count
    case _ =>
      throw new UnsupportedOperationException(
        s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }
  def value():BigInt = {
    this.num
  }
  def copy(): AccumulatorV2[BigInt,BigInt] = {
    new EvenAccumulator
  }
  def isZero():Boolean = {
    this.num == 0
  }
}
