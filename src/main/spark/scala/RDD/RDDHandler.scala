package scala.RDD

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import sys.process._

object RDDHandler {
  val spark = SparkSession.builder().master("local[*]").getOrCreate()
  import spark.implicits._
  def main(args: Array[String]): Unit ={
    //interoperate()
    //createRddFromLocalCollection()
    //transformations()
    //actionsHandle()
    //saveData()
    //cacheHandle()
    //checkpointHandle()
    //pipeHandle()
    val myCollection = "Spark The Definitive Guide : Big Data Processing Made Simple"
      .split(" ")
    val words = spark.sparkContext.parallelize(myCollection, 4)
    words.pipe("cmd.exe /C find 'Spark'").collect()
  }

  def interoperate(): Unit ={
    import spark.implicits._
    //converts a Dataset[Long] to RDD[Long]
    spark.range(500).toDF().rdd.map(x => x)
    //convert this Row object to the correct data type or extract values out of it
    spark.range(10).toDF().rdd.map(rowObject => rowObject.getLong(0))
    //n use the same methodology to create a DataFrame or Dataset from an RDD
    spark.range(10).toDF().rdd.map(rowObject => rowObject.getLong(0)).toDF().show(3)
    //+-----+
    //|value|
    //+-----+
    //|    0|
    //|    1|
    //|    2|
    //+-----+
  }

  def createRddFromLocalCollection(): Unit ={
    val myCollection = "Spark The Definitive Guide : Big Data Processing Made Simple"
      .split(" ")
    val words = spark.sparkContext.parallelize(myCollection, 2)
    words.toDF().show()
    //+----------+
    //|     value|
    //+----------+
    //|     Spark|
    //|       The|
    //|Definitive|
    //|     Guide|
    //|         :|
    //|       Big|
    //|      Data|
    //|Processing|
    //|      Made|
    //|    Simple|
    //+----------+
    //An additional feature is that you can then name this RDD to show up in the Spark UI according to a given name
    words.setName("myWords")
    println(words.name)
  }

  def transformations(): Unit ={
    val myCollection = "Spark The Definitive Guide : Big Data Processing Made Simple"
      .split(" ")
    val words = spark.sparkContext.parallelize(myCollection, 2)
    /**
     * distinct
     */
    println(words.distinct().count()) //10

    /**
     * filter
     */
    def startsWithS(individual:String) = {
      individual.startsWith("S")
    }
    words.filter(word => startsWithS(word)).toDF().show(5)
    //+------+
    //| value|
    //+------+
    //| Spark|
    //|Simple|
    //+------+
    /**
     * map
     */
    val words2 = words.map(word => (word, word(0), word.startsWith("S")))

    /**
     * words2 can't apply words2.toDF() now
     Exception in thread "main" java.lang.UnsupportedOperationException: No Encoder found for Char
    - field (class: "scala.Char", name: "_2")
    - root class: "scala.Tuple3"
     if you want to convert to DF
         words2.map(x => (x._1, x._2.toString, x._3)).toDF().show()
     */
    words2.filter(record => record._3).take(5).foreach(print)
    //(Spark,S,true)(Simple,S,true)
    /**
     * flatMap
     * flatMap requires that the ouput of the map function be an iterable that can be expanded
     */
    words.flatMap(word => word.toSeq).take(5).foreach(print)
    //Spark
    words.flatMap(word => word.toSeq).map(x => x.toString).toDF().show(5)
    //+-----+
    //|value|
    //+-----+
    //|    S|
    //|    p|
    //|    a|
    //|    r|
    //|    k|
    //+-----+
    /**
     * sort
     */
    words.sortBy(word => word.length() * -1).take(2).foreach(print)
    //Definitive Processing
    /**
     * Random Splits
     * randomly split an RDD into an Array of RDDs by using the randomSplit method
     * returns an array of RDDs that you can manipulate individually
     */
    val fiftyFiftySplit = words.randomSplit(Array[Double](0.5, 0.5))
    fiftyFiftySplit.foreach(x => x.toDF().show())
    //+----------+
    //|     value|
    //+----------+
    //|       The|
    //|         :|
    //|      Data|
    //|Processing|
    //+----------+
    //+----------+
    //|     value|
    //+----------+
    //|     Spark|
    //|Definitive|
    //|     Guide|
    //|       Big|
    //|      Made|
    //|    Simple|
    //+----------+
  }

  def actionsHandle(): Unit ={
    val myCollection = "Spark The Definitive Guide : Big Data Processing Made Simple"
      .split(" ")
    val words = spark.sparkContext.parallelize(myCollection, 2)
    /**
     * reduce
     */
    println(spark.sparkContext.parallelize(1 to 20).reduce(_ + _)) //210
    //get the longest word
    /**
     * This reducer is a good example because you can get one of two outputs. Because the reduce
operation on the partitions is not deterministic, you can have either “definitive” or “processing”
(both of length 10) as the “left” word. This means that sometimes you can end up with one,
whereas other times you end up with the other.
     * @param leftWord
     * @param rightWord
     * @return
     */
    def wordLengthReducer(leftWord:String, rightWord:String): String = {
      if (leftWord.length > rightWord.length)
        return leftWord
      else
        return rightWord
    }
    words.reduce(wordLengthReducer).foreach(print) //Processing

    /**
     * count
     */
    println(words.count()) //10
    /**
     * countApprox
     * confidence is the probability that the error bounds of the result will contain the true value
     *  countApprox were called repeatedly with confidence 0.9, we would expect 90% of the results to contain the true count.
     */
    val confidence = 0.95
    val timeoutMilliseconds = 400
    println(words.countApprox(timeoutMilliseconds, confidence)) //(final: [10.000, 10.000])
    /**
     * countApproxDistinct
     * There are two implementations of this, both based on streamlib’s implementation of
“HyperLogLog in Practice: Algorithmic Engineering of a State-of-the-Art Cardinality Estimation
Algorithm.”
     *  In the first implementation, the argument we pass into the function is the relative accuracy.
Smaller values create counters that require more space. The value must be greater than 0.000017
     */
    println(words.countApproxDistinct(0.05)) //10
    /**
     * countApproxDistinct
     *  you specify the relative accuracy based on two parameters:
     *  one for “regular” data and another for a sparse representation.
The two arguments are p and sp where p is precision and sp is sparse precision. The relative
accuracy is approximately 1.054 / sqrt(2 ). Setting a nonzero (sp > p) can reduce the
memory consumption and increase accuracy when the cardinality is small. Both values are
integers
     */
    println(words.countApproxDistinct(4, 10)) //10
    /**
     * countByValue
     * You should use this method only if the resulting map is expected to be small
     * because the entire thing is loaded into the driver’s memory
     */
    println(words.countByValue())
    //Map(Definitive -> 1, Simple -> 1, Processing -> 1, The -> 1, Spark -> 1, Made -> 1, Guide -> 1, Big -> 1, : -> 1, Data -> 1)
    /**
     * countByValueApprox
     */
    println(words.countByValueApprox(1000, 0.95))
    //(final: Map(
    // Definitive -> [1.000, 1.000], Simple -> [1.000, 1.000],
    // Processing -> [1.000, 1.000], The -> [1.000, 1.000],
    // Spark -> [1.000, 1.000], Made -> [1.000, 1.000],
    // Guide -> [1.000, 1.000], Big -> [1.000, 1.000],
    // : -> [1.000, 1.000], Data -> [1.000, 1.000]))
    /**
     * first
     */
    println(words.first()) //Spark

    /**
     * max and min
     */
    println(spark.sparkContext.parallelize(1 to 20).max()) //20
    println(spark.sparkContext.parallelize(1 to 20).min()) // 1

    /**
     * take
     * This works by first scanning one partition and then using the results from that partition
     * to estimate the number of additional partitions needed to satisfy the limit
     */
    words.take(5).foreach(print) //Spark The Definitive Guide :
    println()
    words.takeOrdered(5).foreach(print) //: Big Data Definitive Guide
    println()
    words.top(5).foreach(print) //The Spark Simple Processing Made
    val withReplacement = true
    val numberToTake = 6
    val randomSeed = 100L
    words.takeSample(withReplacement, numberToTake, randomSeed).foreach(print)
    //Guide Spark : Simple Simple Spark
  }

  def saveData(): Unit ={
    val myCollection = "Spark The Definitive Guide : Big Data Processing Made Simple"
      .split(" ")
    val words = spark.sparkContext.parallelize(myCollection, 4)
    println(words.getNumPartitions) //4
    //words.saveAsTextFile("tmp/bookTitle")
    words.saveAsObjectFile("tmp/my/sequenceFilePath")
  }

  def cacheHandle(): Unit ={
    val myCollection = "Spark The Definitive Guide : Big Data Processing Made Simple"
      .split(" ")
    val words = spark.sparkContext.parallelize(myCollection, 4)
    println(words.getStorageLevel) //StorageLevel(1 replicas)
    words.cache()
    println(words.getStorageLevel) //StorageLevel(memory, deserialized, 1 replicas)
    words.persist()
    println(words.getStorageLevel) //StorageLevel(memory, deserialized, 1 replicas)
  }

  def checkpointHandle(): Unit ={
    val myCollection = "Spark The Definitive Guide : Big Data Processing Made Simple"
      .split(" ")
    val words = spark.sparkContext.parallelize(myCollection, 4)
    spark.sparkContext.setCheckpointDir("spark-checkpoints/words")
    words.checkpoint()
  }

  def pipeHandle(): Unit ={
    val myCollection = "Spark The Definitive Guide : Big Data Processing Made Simple"
      .split(" ")
    val words = spark.sparkContext.parallelize(myCollection, 4)
    //words.pipe("wc -l").collect()
    println(words.mapPartitions(part => Iterator[Int](1)).sum()) //4.0
    /**
     * Other functions similar to mapPartitions include mapPartitionsWithIndex. With this you
specify a function that accepts an index (within the partition) and an iterator that goes through all
items within the partition. The partition index is the partition number in your RDD, which
identifies where each record in our dataset sits (and potentially allows you to debug). You might
use this to test whether your map functions are behaving correctly:
     */
    def indexedFunc(partitionIndex:Int, withinPartIterator: Iterator[String]) = {
      withinPartIterator.toList.map(
        value => s"Partition: $partitionIndex => $value").iterator
    }
    words.mapPartitionsWithIndex(indexedFunc).collect().foreach(print)
    //Partition: 0 => Spark Partition: 0 => The
    // Partition: 1 => Definitive Partition: 1 => Guide Partition: 1 => :
    // Partition: 2 => Big Partition: 2 => Data
    // Partition: 3 => ProcessingPartition: 3 => Made Partition: 3 => Simple
//    words.foreachPartition { iter =>
//      import java.io._
//      import scala.util.Random
//      val randomFileName = new Random().nextInt()
//      val pw = new PrintWriter(new File(s"tmp/random-file-${randomFileName}.txt"))
//      while (iter.hasNext) {
//        pw.write(iter.next())
//      }
//      pw.close()
//    }
    spark.sparkContext.parallelize(Seq("Hello", "World"), 2).glom().collect().foreach(x => x.foreach(println))
    // Hello
    // World
  }

}
