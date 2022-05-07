package scala

import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object JoinHandler {
  val spark = SparkSession.builder().master("local").getOrCreate()
  import spark.implicits._
  spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
  var person : DataFrame = null
  var graduateProgram : DataFrame = null
  var sparkStatus : DataFrame = null
  /**
   * Supported join types include:
   * 'inner', 'outer', 'full', 'fullouter', 'full_outer',
   * 'leftouter', 'left', 'left_outer', 'rightouter', 'right',
   * 'right_outer', 'leftsemi', 'left_semi', 'semi', 'leftanti',
   * 'left_anti', 'anti', 'cross'
   */
  var joinType = ""
  var joinExpression : Column = null

  def main(args: Array[String]): Unit ={
    loadData()
    //innerJoin()
    //outerJoin()
    //leftOuterJoin()
    //rightOuterJoin()
    //leftSemiJoin()
    //leftAntiJoin()
    //naturalJoin()
    //crossJoin()
    //join_on_complex_types()
    duplicate_col_handle()
  }
  def loadData(): Unit ={
    import spark.implicits._
    person = Seq(
      (0, "Bill Chambers", 0, Seq(100)),
      (1, "Matei Zaharia", 1, Seq(500, 250, 100)),
      (2, "Michael Armbrust", 1, Seq(250, 100)))
      .toDF("id", "name", "graduate_program", "spark_status")
    graduateProgram = Seq(
      (0, "Masters", "School of Information", "UC Berkeley"),
      (2, "Masters", "EECS", "UC Berkeley"),
      (1, "Ph.D.", "EECS", "UC Berkeley"))
      .toDF("id", "degree", "department", "school")
    sparkStatus = Seq(
      (500, "Vice President"),
      (250, "PMC Member"),
      (100, "Contributor"))
      .toDF("id", "status")
    person.createOrReplaceTempView("person")
    graduateProgram.createOrReplaceTempView("graduateProgram")
    sparkStatus.createOrReplaceTempView("sparkStatus")
    joinExpression = person.col("graduate_program") === graduateProgram.col("id")
  }

  def innerJoin(): Unit ={
    val wrongJoinExpression = person.col("name") === graduateProgram.col("school")
    joinType = "inner"
    person.join(graduateProgram, joinExpression).show()
    person.join(graduateProgram, joinExpression, joinType).show()
    //+---+----------------+----------------+---------------+---+-------+--------------------+-----------+
    //| id|            name|graduate_program|   spark_status| id| degree|          department|     school|
    //+---+----------------+----------------+---------------+---+-------+--------------------+-----------+
    //|  0|   Bill Chambers|               0|          [100]|  0|Masters|School of Informa...|UC Berkeley|
    //|  2|Michael Armbrust|               1|     [250, 100]|  1|  Ph.D.|                EECS|UC Berkeley|
    //|  1|   Matei Zaharia|               1|[500, 250, 100]|  1|  Ph.D.|                EECS|UC Berkeley|
    //+---+----------------+----------------+---------------+---+-------+--------------------+-----------+
  }

  def outerJoin(): Unit ={
    joinType = "outer"
    person.join(graduateProgram, joinExpression, joinType).show()
    //+----+----------------+----------------+---------------+---+-------+--------------------+-----------+
    //|  id|            name|graduate_program|   spark_status| id| degree|          department|     school|
    //+----+----------------+----------------+---------------+---+-------+--------------------+-----------+
    //|   1|   Matei Zaharia|               1|[500, 250, 100]|  1|  Ph.D.|                EECS|UC Berkeley|
    //|   2|Michael Armbrust|               1|     [250, 100]|  1|  Ph.D.|                EECS|UC Berkeley|
    //|null|            null|            null|           null|  2|Masters|                EECS|UC Berkeley|
    //|   0|   Bill Chambers|               0|          [100]|  0|Masters|School of Informa...|UC Berkeley|
    //+----+----------------+----------------+---------------+---+-------+--------------------+-----------+
  }

  def leftOuterJoin(): Unit ={
    joinType = "left_outer"
    graduateProgram.join(person, joinExpression, joinType).show()
    //+---+-------+--------------------+-----------+----+----------------+----------------+---------------+
    //| id| degree|          department|     school|  id|            name|graduate_program|   spark_status|
    //+---+-------+--------------------+-----------+----+----------------+----------------+---------------+
    //|  0|Masters|School of Informa...|UC Berkeley|   0|   Bill Chambers|               0|          [100]|
    //|  2|Masters|                EECS|UC Berkeley|null|            null|            null|           null|
    //|  1|  Ph.D.|                EECS|UC Berkeley|   2|Michael Armbrust|               1|     [250, 100]|
    //|  1|  Ph.D.|                EECS|UC Berkeley|   1|   Matei Zaharia|               1|[500, 250, 100]|
    //+---+-------+--------------------+-----------+----+----------------+----------------+---------------+
    val gradProgram2 = graduateProgram.union(Seq(
      (0, "Masters", "Duplicated", "Duplicated")).toDF())
    person.join(gradProgram2, joinExpression, joinType).show()
    //+---+----------------+----------------+---------------+---+-------+--------------------+-----------+
    //| id|            name|graduate_program|   spark_status| id| degree|          department|     school|
    //+---+----------------+----------------+---------------+---+-------+--------------------+-----------+
    //|  0|   Bill Chambers|               0|          [100]|  0|Masters|          Duplicated| Duplicated|
    //|  0|   Bill Chambers|               0|          [100]|  0|Masters|School of Informa...|UC Berkeley|
    //|  1|   Matei Zaharia|               1|[500, 250, 100]|  1|  Ph.D.|                EECS|UC Berkeley|
    //|  2|Michael Armbrust|               1|     [250, 100]|  1|  Ph.D.|                EECS|UC Berkeley|
    //+---+----------------+----------------+---------------+---+-------+--------------------+-----------+
  }

  def rightOuterJoin(): Unit ={
    joinType = "right_outer"
    person.join(graduateProgram, joinExpression, joinType).show()
    //+----+----------------+----------------+---------------+---+-------+--------------------+-----------+
    //|  id|            name|graduate_program|   spark_status| id| degree|          department|     school|
    //+----+----------------+----------------+---------------+---+-------+--------------------+-----------+
    //|   0|   Bill Chambers|               0|          [100]|  0|Masters|School of Informa...|UC Berkeley|
    //|null|            null|            null|           null|  2|Masters|                EECS|UC Berkeley|
    //|   2|Michael Armbrust|               1|     [250, 100]|  1|  Ph.D.|                EECS|UC Berkeley|
    //|   1|   Matei Zaharia|               1|[500, 250, 100]|  1|  Ph.D.|                EECS|UC Berkeley|
    //+----+----------------+----------------+---------------+---+-------+--------------------+-----------+
    graduateProgram.join(person, joinExpression, joinType).show()
    //+---+-------+--------------------+-----------+---+----------------+----------------+---------------+
    //| id| degree|          department|     school| id|            name|graduate_program|   spark_status|
    //+---+-------+--------------------+-----------+---+----------------+----------------+---------------+
    //|  0|Masters|School of Informa...|UC Berkeley|  0|   Bill Chambers|               0|          [100]|
    //|  1|  Ph.D.|                EECS|UC Berkeley|  1|   Matei Zaharia|               1|[500, 250, 100]|
    //|  1|  Ph.D.|                EECS|UC Berkeley|  2|Michael Armbrust|               1|     [250, 100]|
    //+---+-------+--------------------+-----------+---+----------------+----------------+---------------+
  }

  def leftSemiJoin(): Unit ={

    joinType = "left_semi"
    graduateProgram.join(person, joinExpression, joinType).show()
    //+---+-------+--------------------+-----------+
    //| id| degree|          department|     school|
    //+---+-------+--------------------+-----------+
    //|  0|Masters|School of Informa...|UC Berkeley|
    //|  1|  Ph.D.|                EECS|UC Berkeley|
    //+---+-------+--------------------+-----------+
    person.join(graduateProgram, joinExpression, joinType).show()
    //+---+----------------+----------------+---------------+
    //| id|            name|graduate_program|   spark_status|
    //+---+----------------+----------------+---------------+
    //|  0|   Bill Chambers|               0|          [100]|
    //|  1|   Matei Zaharia|               1|[500, 250, 100]|
    //|  2|Michael Armbrust|               1|     [250, 100]|
    //+---+----------------+----------------+---------------+
    val gradProgram2 = graduateProgram.union(Seq(
      (0, "Masters", "Duplicated Row", "Duplicated School")).toDF())
    gradProgram2.createOrReplaceTempView("gradProgram2")
    gradProgram2.join(person, joinExpression, joinType).show()
    //+---+-------+--------------------+-----------------+
    //| id| degree|          department|           school|
    //+---+-------+--------------------+-----------------+
    //|  0|Masters|School of Informa...|      UC Berkeley|
    //|  1|  Ph.D.|                EECS|      UC Berkeley|
    //|  0|Masters|      Duplicated Row|Duplicated School|
    //+---+-------+--------------------+-----------------+
  }

  def leftAntiJoin(): Unit ={
    joinType = "left_anti"
    graduateProgram.join(person, joinExpression, joinType).show()
    //+---+-------+----------+-----------+
    //| id| degree|department|     school|
    //+---+-------+----------+-----------+
    //|  2|Masters|      EECS|UC Berkeley|
    //+---+-------+----------+-----------+
    val gradProgram2 = graduateProgram.union(Seq(
      (0, "Masters", "Duplicated Row", "Duplicated School"),
      (5, "Masters", "Duplicated Row", "Duplicated School")).toDF())
    gradProgram2.createOrReplaceTempView("gradProgram2")
    gradProgram2.join(person, joinExpression, joinType).show()
    //+---+-------+--------------+-----------------+
    //| id| degree|    department|           school|
    //+---+-------+--------------+-----------------+
    //|  2|Masters|          EECS|      UC Berkeley|
    //|  5|Masters|Duplicated Row|Duplicated School|
    //+---+-------+--------------+-----------------+
  }

  def naturalJoin(): Unit ={
    spark.sql("SELECT * FROM graduateProgram NATURAL JOIN person").show()
    //+---+-------+--------------------+-----------+----------------+----------------+---------------+
    //| id| degree|          department|     school|            name|graduate_program|   spark_status|
    //+---+-------+--------------------+-----------+----------------+----------------+---------------+
    //|  0|Masters|School of Informa...|UC Berkeley|   Bill Chambers|               0|          [100]|
    //|  2|Masters|                EECS|UC Berkeley|Michael Armbrust|               1|     [250, 100]|
    //|  1|  Ph.D.|                EECS|UC Berkeley|   Matei Zaharia|               1|[500, 250, 100]|
    //+---+-------+--------------------+-----------+----------------+----------------+---------------+
  }

  def crossJoin(): Unit ={
    joinType = "cross"
    graduateProgram.join(person, joinExpression, joinType).show()
    //+---+-------+--------------------+-----------+---+----------------+----------------+---------------+
    //| id| degree|          department|     school| id|            name|graduate_program|   spark_status|
    //+---+-------+--------------------+-----------+---+----------------+----------------+---------------+
    //|  0|Masters|School of Informa...|UC Berkeley|  0|   Bill Chambers|               0|          [100]|
    //|  1|  Ph.D.|                EECS|UC Berkeley|  2|Michael Armbrust|               1|     [250, 100]|
    //|  1|  Ph.D.|                EECS|UC Berkeley|  1|   Matei Zaharia|               1|[500, 250, 100]|
    //+---+-------+--------------------+-----------+---+----------------+----------------+---------------+
    person.crossJoin(graduateProgram).show()
    //+---+----------------+----------------+---------------+---+-------+--------------------+-----------+
    //| id|            name|graduate_program|   spark_status| id| degree|          department|     school|
    //+---+----------------+----------------+---------------+---+-------+--------------------+-----------+
    //|  0|   Bill Chambers|               0|          [100]|  0|Masters|School of Informa...|UC Berkeley|
    //|  1|   Matei Zaharia|               1|[500, 250, 100]|  0|Masters|School of Informa...|UC Berkeley|
    //|  2|Michael Armbrust|               1|     [250, 100]|  0|Masters|School of Informa...|UC Berkeley|
    //|  0|   Bill Chambers|               0|          [100]|  2|Masters|                EECS|UC Berkeley|
    //|  1|   Matei Zaharia|               1|[500, 250, 100]|  2|Masters|                EECS|UC Berkeley|
    //|  2|Michael Armbrust|               1|     [250, 100]|  2|Masters|                EECS|UC Berkeley|
    //|  0|   Bill Chambers|               0|          [100]|  1|  Ph.D.|                EECS|UC Berkeley|
    //|  1|   Matei Zaharia|               1|[500, 250, 100]|  1|  Ph.D.|                EECS|UC Berkeley|
    //|  2|Michael Armbrust|               1|     [250, 100]|  1|  Ph.D.|                EECS|UC Berkeley|
    //+---+----------------+----------------+---------------+---+-------+--------------------+-----------+
  }

  /**
   * more complex method
   */

  def join_on_complex_types(): Unit ={
    import org.apache.spark.sql.functions.expr
    person.withColumnRenamed("id", "personId")
      .join(sparkStatus, expr("array_contains(spark_status, id)")).show()
    //+--------+----------------+----------------+---------------+---+--------------+
    //|personId|            name|graduate_program|   spark_status| id|        status|
    //+--------+----------------+----------------+---------------+---+--------------+
    //|       0|   Bill Chambers|               0|          [100]|100|   Contributor|
    //|       1|   Matei Zaharia|               1|[500, 250, 100]|500|Vice President|
    //|       1|   Matei Zaharia|               1|[500, 250, 100]|250|    PMC Member|
    //|       1|   Matei Zaharia|               1|[500, 250, 100]|100|   Contributor|
    //|       2|Michael Armbrust|               1|     [250, 100]|250|    PMC Member|
    //|       2|Michael Armbrust|               1|     [250, 100]|100|   Contributor|
    //+--------+----------------+----------------+---------------+---+--------------+
  }

  def duplicate_col_handle(): Unit ={
    val gradProgramDupe = graduateProgram.withColumnRenamed("id", "graduate_program")
    var joinExpr = gradProgramDupe.col("graduate_program") === person.col(
      "graduate_program")
    person.join(gradProgramDupe, joinExpr).show()
    //+---+----------------+----------------+---------------+----------------+-------+--------------------+-----------+
    //| id|            name|graduate_program|   spark_status|graduate_program| degree|          department|     school|
    //+---+----------------+----------------+---------------+----------------+-------+--------------------+-----------+
    //|  0|   Bill Chambers|               0|          [100]|               0|Masters|School of Informa...|UC Berkeley|
    //|  2|Michael Armbrust|               1|     [250, 100]|               1|  Ph.D.|                EECS|UC Berkeley|
    //|  1|   Matei Zaharia|               1|[500, 250, 100]|               1|  Ph.D.|                EECS|UC Berkeley|
    //+---+----------------+----------------+---------------+----------------+-------+--------------------+-----------+
    //The challenge arises when we refer to one of these columns:
    //person.join(gradProgramDupe, joinExpr).select("graduate_program").show() //false
    /**
     * specific col, my Method
     */
    person.join(gradProgramDupe, joinExpr, "right_outer").select(gradProgramDupe("graduate_program")).show() //true
    //+------person----++granProgramDupe+
    //+----------------++----------------+
    //|graduate_program||graduate_program|
    //+----------------++----------------+
    //|               0||               0|
    //|            null||               2|
    //|               1||               1|
    //|               1||               1|
    //+----------------++----------------+
    /**
     * Different join expression
     */
    person.join(gradProgramDupe,"graduate_program").select("graduate_program").show()
    /**
     * Dropping the column after the join
     */
    person.join(gradProgramDupe, joinExpr).drop(person.col("graduate_program"))
      .select("graduate_program").show()
    //+----------------+
    //|graduate_program|
    //+----------------+
    //|               0|
    //|               1|
    //|               1|
    //+----------------+
    joinExpr = person.col("graduate_program") === graduateProgram.col("id")
    person.join(graduateProgram, joinExpr).drop(graduateProgram.col("id")).show()
    /**
     * Notice how the column uses the .col method instead of a column function.
     * That allows us to implicitly specify that column by its specific ID.
     */
    //+---+----------------+----------------+---------------+-------+--------------------+-----------+
    //| id|            name|graduate_program|   spark_status| degree|          department|     school|
    //+---+----------------+----------------+---------------+-------+--------------------+-----------+
    //|  0|   Bill Chambers|               0|          [100]|Masters|School of Informa...|UC Berkeley|
    //|  2|Michael Armbrust|               1|     [250, 100]|  Ph.D.|                EECS|UC Berkeley|
    //|  1|   Matei Zaharia|               1|[500, 250, 100]|  Ph.D.|                EECS|UC Berkeley|
    //+---+----------------+----------------+---------------+-------+--------------------+-----------+
    /**
     *  Renaming a column before the join
     */
    val gradProgram3 = graduateProgram.withColumnRenamed("id", "grad_id")
    joinExpr = person.col("graduate_program") === gradProgram3.col("grad_id")
    person.join(gradProgram3, joinExpr).show()
    //+---+----------------+----------------+---------------+-------+-------+--------------------+-----------+
    //| id|            name|graduate_program|   spark_status|grad_id| degree|          department|     school|
    //+---+----------------+----------------+---------------+-------+-------+--------------------+-----------+
    //|  0|   Bill Chambers|               0|          [100]|      0|Masters|School of Informa...|UC Berkeley|
    //|  2|Michael Armbrust|               1|     [250, 100]|      1|  Ph.D.|                EECS|UC Berkeley|
    //|  1|   Matei Zaharia|               1|[500, 250, 100]|      1|  Ph.D.|                EECS|UC Berkeley|
    //+---+----------------+----------------+---------------+-------+-------+--------------------+-----------+
  }

}
