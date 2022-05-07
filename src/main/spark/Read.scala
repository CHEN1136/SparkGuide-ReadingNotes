import org.apache.spark.sql.SparkSession

import java.lang.Thread.sleep

object Read {


  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir","D:\\Hadoop")
    val spark = SparkSession.builder().master("local").getOrCreate()
    val data = spark.read.option("inferSchema","true").option("header","true").csv("src/data/flight-data/csv/2015-summary.csv")
    val d = data.limit(5)
    spark.conf.set("spark.sql.shuffle.partitions", "200")
    data.sort("count").explain()
    d.show()
    sleep(20000)
  }
}
