package scala.ProductionAPP

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr

import java.util.concurrent.Executors
import scala.ProductionAPP.LifeCycle.spark

object ThreadScheduleHandler {
  val conf = new SparkConf()
  conf.set("spark.scheduler.mode", "FAIR")
  val spark = SparkSession.builder().config(conf).master("local[*]").getOrCreate()

  def main(args: Array[String]): Unit ={
    val df1 = spark.range(2, 10000000, 2)
    val df2 = spark.range(2, 10000000, 4)
    val step1 = df1.repartition(5)
    val step12 = df2.repartition(6)
    val step2 = step1.selectExpr("id * 5 as id2")
    val step3 = step2.join(step12, step2("id2") === step12("id"))
    val step4 = step3.select(expr("sum(id)"))
    step3.cache()

    val jobExecutor = Executors.newFixedThreadPool(2)

    jobExecutor.execute(new Runnable {
      override def run(): Unit = {
        spark.sparkContext.setLocalProperty("spark.scheduler.pool", "count-pool")
        step3.select(expr("avg(id)")).collect()
      }
    })

    jobExecutor.execute(new Runnable {
      override def run(): Unit = {
        spark.sparkContext.setLocalProperty("spark.scheduler.pool", "take-pool")
        step3.select(expr("sum(id)")).collect()
      }
    })
    jobExecutor.shutdown()
    while (!jobExecutor.isTerminated) {}
    println("Done!")
    while(true){}
  }
}
