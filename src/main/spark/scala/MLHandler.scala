package scala

import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import scala.DateSetHandler.Flight
import scala.StreamProcessing.StructuredStreamingHandler.spark

object MLHandler {
  val spark = SparkSession.builder().master("local").getOrCreate()
  val staticDataFrame = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("src/data/retail-data/by-day/*.csv")

  def main( args:Array[String]): Unit ={
    import org.apache.spark.sql.functions.date_format
    import spark.implicits._

    val preppedDataFrame = staticDataFrame
      .na.fill(0)
      .withColumn("day_of_week", date_format($"InvoiceDate", "EEEE"))
      .coalesce(5)

    //划分训练和测试数据
    val trainDataFrame = preppedDataFrame
      .where("InvoiceDate < '2011-07-01'")
    val testDataFrame = preppedDataFrame
      .where("InvoiceDate >= '2011-07-01'")

    //转换器
    import org.apache.spark.ml.feature.StringIndexer
    val indexer = new StringIndexer()
      .setInputCol("day_of_week")
      .setOutputCol("day_of_week_index")

    import org.apache.spark.ml.feature.OneHotEncoder
    val encoder = new OneHotEncoder()
      .setInputCol("day_of_week_index")
      .setOutputCol("day_of_week_encoded")

    //输入向量
    import org.apache.spark.ml.feature.VectorAssembler
    val vectorAssembler = new VectorAssembler()
      .setInputCols(Array("UnitPrice", "Quantity", "day_of_week_encoded"))
      .setOutputCol("features")

    import org.apache.spark.ml.Pipeline
    val transformationPipeline = new Pipeline()
      .setStages(Array(indexer, encoder, vectorAssembler))

    val fittedPipeline = transformationPipeline.fit(trainDataFrame)

    val transformedTraining = fittedPipeline.transform(trainDataFrame)
    //trainDataFrame.show(5)
//    +---------+---------+--------------------+--------+-------------------+---------+----------+--------------+-----------+
//    |InvoiceNo|StockCode|         Description|Quantity|        InvoiceDate|UnitPrice|CustomerID|       Country|day_of_week|
//    +---------+---------+--------------------+--------+-------------------+---------+----------+--------------+-----------+
//    |   537226|    22811|SET OF 6 T-LIGHTS...|       6|2010-12-06 08:34:00|     2.95|   15987.0|United Kingdom|     Monday|
//    |   537226|    21713|CITRONELLA CANDLE...|       8|2010-12-06 08:34:00|      2.1|   15987.0|United Kingdom|     Monday|
//    |   537226|    22927|GREEN GIANT GARDE...|       2|2010-12-06 08:34:00|     5.95|   15987.0|United Kingdom|     Monday|
//    |   537226|    20802|SMALL GLASS SUNDA...|       6|2010-12-06 08:34:00|     1.65|   15987.0|United Kingdom|     Monday|
//    |   537226|    22052|VINTAGE CARAVAN G...|      25|2010-12-06 08:34:00|     0.42|   15987.0|United Kingdom|     Monday|
//    +---------+---------+--------------------+--------+-------------------+---------+----------+--------------+-----------+
    //transformedTraining.select(col("features")).show(5,false)
    //+---------------------------+
    //|features                   |
    //+---------------------------+
    //|(7,[0,1,4],[2.95,6.0,1.0]) |
    //|(7,[0,1,4],[2.1,8.0,1.0])  |
    //|(7,[0,1,4],[5.95,2.0,1.0]) |
    //|(7,[0,1,4],[1.65,6.0,1.0]) |
    //|(7,[0,1,4],[0.42,25.0,1.0])|
    //+---------------------------+

    //using caching,
    // This will put a copy of the intermediately transformed dataset into memory,
    // allowing us to repeatedly access it at much lower cost than running the entire pipeline again
    transformedTraining.cache()


    import org.apache.spark.ml.clustering.KMeans
    //train a k-mean model
    val kmeans = new KMeans()
      .setK(20)
      .setSeed(1L)
    val kmModel = kmeans.fit(transformedTraining)

    //make predictions
    val predictions = kmModel.transform(fittedPipeline.transform(testDataFrame))
    //predictions.show(5, false)
    //+---------+---------+-------------------------------+--------+-------------------+---------+----------+--------------+-----------+-----------------+-------------------+---------------------------+----------+
    //|InvoiceNo|StockCode|Description                    |Quantity|InvoiceDate        |UnitPrice|CustomerID|Country       |day_of_week|day_of_week_index|day_of_week_encoded|features                   |prediction|
    //+---------+---------+-------------------------------+--------+-------------------+---------+----------+--------------+-----------+-----------------+-------------------+---------------------------+----------+
    //|580538   |23084    |RABBIT NIGHT LIGHT             |48      |2011-12-05 08:38:00|1.79     |14075.0   |United Kingdom|Monday     |2.0              |(5,[2],[1.0])      |(7,[0,1,4],[1.79,48.0,1.0])|13        |
    //|580538   |23077    |DOUGHNUT LIP GLOSS             |20      |2011-12-05 08:38:00|1.25     |14075.0   |United Kingdom|Monday     |2.0              |(5,[2],[1.0])      |(7,[0,1,4],[1.25,20.0,1.0])|13        |
    //|580538   |22906    |12 MESSAGE CARDS WITH ENVELOPES|24      |2011-12-05 08:38:00|1.65     |14075.0   |United Kingdom|Monday     |2.0              |(5,[2],[1.0])      |(7,[0,1,4],[1.65,24.0,1.0])|13        |
    //|580538   |21914    |BLUE HARMONICA IN BOX          |24      |2011-12-05 08:38:00|1.25     |14075.0   |United Kingdom|Monday     |2.0              |(5,[2],[1.0])      |(7,[0,1,4],[1.25,24.0,1.0])|13        |
    //|580538   |22467    |GUMBALL COAT RACK              |6       |2011-12-05 08:38:00|2.55     |14075.0   |United Kingdom|Monday     |2.0              |(5,[2],[1.0])      |(7,[0,1,4],[2.55,6.0,1.0]) |0

    // in Scala
    //val transformedTest = fittedPipeline.transform(testDataFrame)

    //computeCost 在3.0版本移除 Evaluate clustering by computing Silhouette score
    val evaluator = new ClusteringEvaluator()
    val silhouette = evaluator.evaluate(predictions)
    // Shows the result.
    println("Cluster Centers: ")
    val centers = kmModel.clusterCenters

    centers.foreach(println)
  }
}
