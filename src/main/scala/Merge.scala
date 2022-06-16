package it.unibo.bd

import it.unibo.bd.utils.Utils.RichRDD
import org.apache.spark.{ SparkConf, SparkContext }

object Merge {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("Merge"))
    println(s"Application started at http://localhost:20888/proxy/${sc.applicationId}/\n")

    val unbalancedDataset = sc.textFile(s"${args(0)}/unbalaced_20_80_dataset.csv")

    val balancedDataset = sc.textFile(s"${args(0)}/final_dataset.csv")

    val totalDataset = unbalancedDataset.skip(1) ++ balancedDataset.skip(1)
    totalDataset.coalesce(1).saveAsTextFile(s"${args(0)}/ddos-dataset.csv")

    val sampledDataset = totalDataset.sample(withReplacement = false, 0.001)
    sampledDataset.coalesce(1).saveAsTextFile(s"${args(0)}/ddos-dataset-sampled.csv")
  }
}
