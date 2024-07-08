// Databricks notebook source
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, sum}

val spark = SparkSession.builder()
.appName("MatrixMultiplication")
.master("local[*]")
.getOrCreate()

    val M = Seq(
      (1, 1, -2.0),
      (0, 0, 5.0),
      (2, 2, 6.0),
      (0, 1, -3.0),
      (3, 2, 7.0),
      (0, 2, -1.0),
      (1, 0, 3.0),
      (1, 2, 4.0),
      (2, 0, 1.0),
      (3, 0, -4.0),
      (3, 1, 2.0)
    ).toDF("i", "j", "value")

    val N = Seq(
      (1, 0, 3.0),
      (0, 0, 5.0),
      (1, 2, -2.0),
      (2, 0, 9.0),
      (0, 1, -3.0),
      (0, 2, -1.0),
      (1, 1, 8.0),
      (2, 1, 4.0)
    ).toDF("i", "j", "value")

M.createOrReplaceTempView("M_table")
N.createOrReplaceTempView("N_table")

  val Output = spark.sql("""
  SELECT m.i, n.j, SUM(m.value * n.value) AS value
  FROM M_table AS m
  JOIN N_table AS n
  ON m.j = n.i
  GROUP BY m.i, n.j
  ORDER BY m.i, n.j
  """)

  Output.show()

