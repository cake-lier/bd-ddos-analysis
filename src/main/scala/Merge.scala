package it.unibo.bd

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.SparkSession

object Merge extends App {
  val sc = new SparkContext(new SparkConf().setAppName("Merge"))

  val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .getOrCreate()

  println(s"Application started at http://localhost:20888/proxy/${sc.applicationId}/\n")

  val unbalancedDataset = spark.read.option("header", value = true).csv(s"s3://${args(0)}/unbalaced_20_80_dataset.csv")
  val balancedDataset = spark.read.csv(s"s3://${args(0)}/final_dataset.csv")

  val unionDataset = balancedDataset.union(unbalancedDataset)

  unionDataset.coalesce(1).write.option("header", value = true).csv(s"s3://${args(0)}/ddos-dataset.csv")

  val sampledDataset = unionDataset.sample(withReplacement = false, 0.001)
  sampledDataset.coalesce(1).write.option("header", value = true).csv(s"s3://${args(0)}/ddos-dataset-sample.csv")
}
