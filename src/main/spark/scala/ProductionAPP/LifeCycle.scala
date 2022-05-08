package scala.ProductionAPP

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.scheduler.SchedulingMode.FAIR
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr}

object LifeCycle {
  val conf = new SparkConf()
  conf.set("spark.scheduler.mode", "FAIR")
  val spark = SparkSession.builder().config(conf).master("local[*]").getOrCreate()
  spark.sparkContext.setLocalProperty("spark.scheduler.pool", "pool1")
  import spark.implicits._

  def main(args: Array[String]): Unit ={
    val df1 = spark.range(2, 10000000, 2)
    val df2 = spark.range(2, 10000000, 4)
    val step1 = df1.repartition(5)
    val step12 = df2.repartition(6)
    val step2 = step1.selectExpr("id * 5 as id2")
    val step3 = step2.join(step12, step2("id2") === step12("id"))
    val step4 = step3.select(expr("sum(id)"))
    step3.cache()
    step3.select(expr("avg(id)")).collect()
    step3.select(expr("sum(id)")).collect()
    step4.collect().foreach(println) //[2500000000000]
    step4.explain()
    //== Physical Plan ==
    //*(7) HashAggregate(keys=[], functions=[sum(id#2L)])
    //+- Exchange SinglePartition, true, [id=#66]
    //   +- *(6) HashAggregate(keys=[], functions=[partial_sum(id#2L)])
    //      +- *(6) Project [id#2L]
    //         +- *(6) SortMergeJoin [id2#8L], [id#2L], Inner
    //            :- *(3) Sort [id2#8L ASC NULLS FIRST], false, 0
    //            :  +- Exchange hashpartitioning(id2#8L, 200), true, [id=#50]
    //            :     +- *(2) Project [(id#0L * 5) AS id2#8L]
    //            :        +- Exchange RoundRobinPartitioning(5), false, [id=#46]
    //            :           +- *(1) Range (2, 10000000, step=2, splits=8)
    //            +- *(5) Sort [id#2L ASC NULLS FIRST], false, 0
    //               +- Exchange hashpartitioning(id#2L, 200), true, [id=#57]
    //                  +- Exchange RoundRobinPartitioning(6), false, [id=#56]
    //                     +- *(4) Range (2, 10000000, step=4, splits=8)
    while(true) {}
  }
}
