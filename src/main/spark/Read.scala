import org.apache.spark.sql.SparkSession

import java.lang.Thread.sleep

object Read {


  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir","D:\\Hadoop")
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("INFO")
    val data = spark.read.option("inferSchema","true").option("header","true").csv("src/data/flight-data/csv/2015-summary.csv")
    //data.groupBy("DEST_COUNTRY_NAME").sum()
    data.repartition(2)
    data.show()
    //data.count()
//    spark.conf.set("spark.sql.shuffle.partitions", "200")
//    data.sort("count").explain()
//    d.show()
//    sleep(20000)
    while (true){}
  }
}
