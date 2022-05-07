package scala.RDD

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col

import scala.util.Random


object AdvancedRDDHandler {
  val spark = SparkSession.builder().master("local[*]").getOrCreate()
  var sc = spark.sparkContext

  import spark.implicits._
  val myCollection = "Spark The Definitive Guide : Big Data Processing Made Simple"
    .split(" ")
  val words = spark.sparkContext.parallelize(myCollection, 2)

  def main(args: Array[String]): Unit ={
    //createKV()
    //aggregationsHandle()
    //coGroupHandle()
    //joinHandler()
    //partitionCtl()

  }

  def createKV(): Unit ={
    words.map(word => (word.toLowerCase, 1)).toDF().show(2)
    //+-----+---+
    //|   _1| _2|
    //+-----+---+
    //|spark|  1|
    //|  the|  1|
    //+-----+---+
    val keyword = words.keyBy(word => word.toLowerCase.toSeq(0).toString)
    keyword.toDF().orderBy(col("_1").desc).show(3)
    //+---+------+
    //| _1|    _2|
    //+---+------+
    //|  t|   The|
    //|  s|Simple|
    //|  s| Spark|
    //+---+------+
    keyword.mapValues(word => word.toUpperCase).collect().foreach(println)
    //(s,SPARK)
    //(t,THE)
    //(d,DEFINITIVE)
    //(g,GUIDE)
    //(:,:)
    //(b,BIG)
    //(d,DATA)
    //(p,PROCESSING)
    //(m,MADE)
    //(s,SIMPLE)
    /**
     * expand the number of rows that you have to make it so that each row represents a character
     * like explode the value to a char
     */
    keyword.flatMapValues(word => word.toUpperCase).collect().foreach(println)
    //(s,S)
    //(s,P)
    //(s,A)
    //(s,R)
    //(s,K)
    //(t,T)
    keyword.keys.collect()
    keyword.values.collect()

    keyword.lookup("s").toDF().show()
    //+------+
    //| value|
    //+------+
    //| Spark|
    //|Simple|
    //+------+

    val distinctChars = words.flatMap(word => word.toLowerCase.toSeq).distinct
      .collect()
    import scala.util.Random
    val sampleMap = distinctChars.map(c => (c, new Random().nextDouble())).toMap
    words.map(word => (word.toLowerCase.toSeq(0), word))
      .sampleByKey(true, sampleMap, 6L)
      .collect()

    words.map(word => (word.toLowerCase.toSeq(0), word))
      .sampleByKeyExact(true, sampleMap, 6L).collect()
  }

  def aggregationsHandle(): Unit ={
    /**
     * load data
     */
    val chars = words.flatMap(word => word.toLowerCase.toSeq)
    val KVcharacters = chars.map(letter => (letter, 1))
    def maxFunc(left:Int, right:Int) = math.max(left, right)
    def addFunc(left:Int, right:Int) = left + right
    val nums = sc.parallelize(1 to 30, 5)
    //countByKey(KVcharacters)
    //groupBy_and_reduce(KVcharacters)
    //nums.saveAsTextFile("tmp/nums") //分区查看, 最大值分别为 6 12 18 24 30 总和为90
    /**
     *  The first aggregates within partitions, the second aggregates across partitions.
     */
    println(nums.aggregate(0)(maxFunc, addFunc)) //90
    println(nums.aggregate(0)(maxFunc, maxFunc)) //30
    println(nums.aggregate(0)(addFunc, addFunc)) //465

    /**
     * 有reduceByKey味道了，继续实现类似reduceByKey效果
     */
    KVcharacters.aggregateByKey(0)(addFunc, addFunc).foreach(print)
    //(s,4)(e,7)(a,4)(i,7)(k,1)(u,1)(o,1)(g,3)(m,2)(c,1)(d,4)(p,3)(t,3)(b,1)(h,1)(n,2)(f,1)(v,1)(:,1)(r,2)(l,1)
    //KVcharacters.saveAsTextFile("tmp/KVcharacters") //分成两个区
    KVcharacters.aggregateByKey(0)(addFunc, maxFunc).collect().foreach(print)
    //(d,2)(p,2)(t,2)(b,1)(h,1)(n,1)(f,1)(v,1)(:,1)(r,1)(l,1)(s,3)(e,4)(a,3)(i,4)(k,1)(u,1)(o,1)(g,2)(m,2)(c,1)
    /**
     * treeAggregate
     * treeAggregate that does the same thing as aggregate (at the user level)
     * but does so in a different way. It basically “pushes down”
     * some of the subaggregations (creating a tree from executor to executor)
     * before performing the final aggregation on the driver.
     */
    val depth = 3
    println(nums.treeAggregate(0)(maxFunc, addFunc, depth)) //90

    /**
     * combineByKey
     * This combiner operates on a given key and merges the values according to some function.
     */
    val valToCombiner = (value:Int) => List(value)
    val mergeValuesFunc = (vals:List[Int], valToAppend:Int) => valToAppend :: vals
    val mergeCombinerFunc = (vals1:List[Int], vals2:List[Int]) => vals1 ::: vals2
    // now we define these as function variables
    val outputPartitions = 6
    val res = KVcharacters.combineByKey(
        valToCombiner,
        mergeValuesFunc,
        mergeCombinerFunc,
        outputPartitions)
    println(res.getNumPartitions) //6
    res.foreach(print)
    //(f,List(1))(r,List(1, 1))(l,List(1))(s,List(1, 1, 1, 1))(a,List(1, 1, 1, 1))(g,List(1, 1, 1))(m,List(1, 1))(e,List(1, 1, 1, 1, 1, 1, 1))
    /**
     * foldByKey
     * foldByKey merges the values for each key using an associative function and a neutral “zero value,”
     *  which can be added to the result an arbitrary number of times, and must not change the result
     *  (e.g., 0 for addition, or 1 for multiplication)
     */
    KVcharacters.foldByKey(0)(addFunc).collect().foreach(print)
    //(d,4)(p,3)(t,3)(b,1)(h,1)(n,2)(f,1)(v,1)(:,1)(r,2)(l,1)(s,4)(e,7)(a,4)(i,7)(k,1)(u,1)(o,1)(g,3)(m,2)(c,1)

  }

  def countByKey(KVcharacters:  RDD[(Char, Int)]): Unit ={
    val timeout = 1000L //milliseconds
    val confidence = 0.95
    KVcharacters.countByKey().foreach(println)
    //(e,7)
    //(s,4)
    //(n,2)
    //(t,3)
    //(u,1)
    println(KVcharacters.countByKeyApprox(timeout, confidence))
    //final: Map(e -> [7.000, 7.000], s -> [4.000, 4.000], n -> [2.000, 2.000], t -> [3.000, 3.000], u -> [1.000, 1.000]
  }

  def groupBy_and_reduce(KVcharacters:  RDD[(Char, Int)]): Unit ={
    def addFunc(left:Int, right:Int) = left + right
    KVcharacters.groupByKey().map(row => (row._1, row._2.reduce(addFunc))).collect().foreach(println)
    KVcharacters.reduceByKey(addFunc).collect().foreach(print) //recommend
    //(d,4)(p,3)(t,3)(b,1)(h,1)(n,2)(f,1)(v,1)(:,1)(r,2)(l,1)(s,4)(e,7)(a,4)(i,7)(k,1)(u,1)(o,1)(g,3)(m,2)(c,1)
  }

  def coGroupHandle(): Unit ={
    import scala.util.Random
    val distinctChars = words.flatMap(word => word.toLowerCase.toSeq).distinct
    val charRDD = distinctChars.map(c => (c, new Random().nextDouble()))
    val charRDD2 = distinctChars.map(c => (c, new Random().nextDouble()))
    val charRDD3 = distinctChars.map(c => (c, new Random().nextDouble()))
    charRDD.cogroup(charRDD2, charRDD3).take(5).foreach(println)
    //(d,(CompactBuffer(0.712732059741726),CompactBuffer(0.0652448690418771),CompactBuffer(0.361572231661929)))
    //(p,(CompactBuffer(0.43600639026926347),CompactBuffer(0.8529280617098401),CompactBuffer(0.9393796625399176)))
    //(t,(CompactBuffer(0.3484979108092584),CompactBuffer(0.8264329346284515),CompactBuffer(0.47785931901197143)))
    //(b,(CompactBuffer(0.45260022261432786),CompactBuffer(0.8962319635607489),CompactBuffer(0.5629007510889266)))
    //(h,(CompactBuffer(0.8970724778298017),CompactBuffer(0.6730143374003479),CompactBuffer(0.8022313767156555)))
  }

  def joinHandler(): Unit ={
    val distinctChars = words.flatMap(word => word.toLowerCase.toSeq).distinct
    val chars = words.flatMap(word => word.toLowerCase.toSeq)
    val KVcharacters = chars.map(letter => (letter, 1))

    val keyedChars = distinctChars.map(c => (c, new Random().nextDouble()))
    val outputPartitions = 10
    KVcharacters.join(keyedChars).count()
    KVcharacters.join(keyedChars, outputPartitions).take(2).foreach(println)
    //(d,(1,0.40632910564886093))
    //(d,(1,0.40632910564886093))
    /**
     * zip, zip allows you to “zip” together two RDDs, assuming that they have the
same length. This creates a PairRDD. The two RDDs must have the same number of partitions as
well as the same number of elements:
     */
//    val numRange = sc.parallelize(0 to 9, 4)
//    //Exception in thread "main" java.lang.IllegalArgumentException: Can't zip RDDs with unequal numbers of partitions: List(2, 4)
    val numRange = sc.parallelize(0 to 9, 2)
    words.zip(numRange).mapPartitions(x => x.map(line => println(line))).collect()
    //(Spark,0)
    //(Big,5)
    //(The,1)
    //(Data,6)
    //(Definitive,2)
    //(Processing,7)
    //(Guide,3)
    //(Made,8)
    //(:,4)
    //(Simple,9)
    //按数字顺序排序就是正确的书名
  }

  def partitionCtl(): Unit ={
//    coalesce()
//    repartition()
    customPartitionHandle()
  }

  def coalesce(): Unit ={
    println(words.coalesce(4).getNumPartitions) //2
    println(words.coalesce(1).getNumPartitions) //1
  }

  def repartition(): Unit ={
    println(words.repartition(10).getNumPartitions) //10
    println(words.repartition(1).getNumPartitions) //1

  }

  def customPartitionHandle(): Unit ={
    val df = spark.read.option("header", "true").option("inferSchema", "true")
      .csv("src/data/retail-data/all/")
    val rdd = df.coalesce(10).rdd

    import org.apache.spark.HashPartitioner
    rdd.map(r => r(6)).take(5).foreach(println)
    //17850
    //17850
    //17850
    //17850
    //17850
    val keyedRDD = rdd.keyBy(row => row(6).asInstanceOf[Int].toDouble)
    keyedRDD
      .partitionBy(new HashPartitioner(4)).map(_._1).glom().map(_.toSet.toSeq.length)
      .take(5).foreach(println)
    //4373
    //0
    //0
    //0
    keyedRDD.partitionBy(new HashPartitioner(10)).take(10).foreach(println)
    //(15100.0,[536374,21258,VICTORIAN SEWING BOX LARGE,32,12/1/2010 9:09,10.95,15100,United Kingdom])
    //(16250.0,[536388,21754,HOME BUILDING BLOCK WORD,3,12/1/2010 9:59,5.95,16250,United Kingdom])
    //(16250.0,[536388,21755,LOVE BUILDING BLOCK WORD,3,12/1/2010 9:59,5.95,16250,United Kingdom])
    //(16250.0,[536388,21523,DOORMAT FANCY FONT HOME SWEET HOME,2,12/1/2010 9:59,7.95,16250,United Kingdom])
    //(16250.0,[536388,21363,HOME SMALL WOOD LETTERS,3,12/1/2010 9:59,4.95,16250,United Kingdom])
    //(16250.0,[536388,21411,GINGHAM HEART  DOORSTOP RED,3,12/1/2010 9:59,4.25,16250,United Kingdom])
    //(16250.0,[536388,22318,FIVE HEART HANGING DECORATION,6,12/1/2010 9:59,2.95,16250,United Kingdom])
    //(16250.0,[536388,22464,HANGING METAL HEART LANTERN,12,12/1/2010 9:59,1.65,16250,United Kingdom])
    //(16250.0,[536388,22915,ASSORTED BOTTLE TOP  MAGNETS ,12,12/1/2010 9:59,0.42,16250,United Kingdom])
    //(16250.0,[536388,22922,FRIDGE MAGNETS US DINER ASSORTED,12,12/1/2010 9:59,0.85,16250,United Kingdom])

    import org.apache.spark.Partitioner
    class DomainPartitioner extends Partitioner {
      def numPartitions = 3
      def getPartition(key: Any): Int = {
        val customerId = key.asInstanceOf[Double].toInt
        if (customerId == 17850.0 || customerId == 12583.0) {
          return 0
        } else {
          return new java.util.Random().nextInt(2) + 1
        }
      }
    }
    keyedRDD
      .partitionBy(new DomainPartitioner)
      .take(10).foreach(println)
    //(17850.0,[536365,85123A,WHITE HANGING HEART T-LIGHT HOLDER,6,12/1/2010 8:26,2.55,17850,United Kingdom])
    //(17850.0,[536365,71053,WHITE METAL LANTERN,6,12/1/2010 8:26,3.39,17850,United Kingdom])
    //(17850.0,[536365,84406B,CREAM CUPID HEARTS COAT HANGER,8,12/1/2010 8:26,2.75,17850,United Kingdom])
    //(17850.0,[536365,84029G,KNITTED UNION FLAG HOT WATER BOTTLE,6,12/1/2010 8:26,3.39,17850,United Kingdom])
    //(17850.0,[536365,84029E,RED WOOLLY HOTTIE WHITE HEART.,6,12/1/2010 8:26,3.39,17850,United Kingdom])
    //(17850.0,[536365,22752,SET 7 BABUSHKA NESTING BOXES,2,12/1/2010 8:26,7.65,17850,United Kingdom])
    //(17850.0,[536365,21730,GLASS STAR FROSTED T-LIGHT HOLDER,6,12/1/2010 8:26,4.25,17850,United Kingdom])
    //(17850.0,[536366,22633,HAND WARMER UNION JACK,6,12/1/2010 8:28,1.85,17850,United Kingdom])
    //(17850.0,[536366,22632,HAND WARMER RED POLKA DOT,6,12/1/2010 8:28,1.85,17850,United Kingdom])
    //(12583.0,[536370,22728,ALARM CLOCK BAKELIKE PINK,24,12/1/2010 8:45,3.75,12583,France])
    /**
     * count of results in each partition
     */
    keyedRDD
      .partitionBy(new DomainPartitioner).map(_._1).glom().map(_.toSet.toSeq.length)
      .take(5).foreach(println)
    //2
    //4306
    //4306
    class myPartitioner extends Partitioner {
      def numPartitions = 4
      def getPartition(key: Any): Int = {
        //key.hashCode() % 4
        //4373
        //0
        //0
        //0
        if (key.asInstanceOf[Double].toInt == 17850.0) return 0
        else return new java.util.Random().nextInt(3) + 1
      }
    }
    keyedRDD
      .partitionBy(new myPartitioner).map(_._1).glom().map(_.toSet.toSeq.length)
      .take(4).foreach(println)
    //1
    //4255
    //4239
    //4228
  }

  def customSerializer(): Unit ={
    class SomeClass extends Serializable {
      var someValue = 0
      def setSomeValue(i:Int) = {
        someValue = i
        this
      }
    }
    sc.parallelize(1 to 10).map(num => new SomeClass().setSomeValue(num)).collect()


  }

}
