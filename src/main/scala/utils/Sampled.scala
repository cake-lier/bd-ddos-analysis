package it.unibo.bd
package utils

import org.apache.spark.rdd.RDD
import org.apache.spark.{ SparkConf, SparkContext }

object Sampled {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("Sampled"))
    println(s"Application started at http://localhost:20888/proxy/${sc.applicationId}/\n")

    val rawDatasetTCP = sc.textFile(s"${args(0)}/DDoS_TCP.csv")
    val rawDatasetUDP = sc.textFile(s"${args(0)}/DDoS_UDP.csv")
    val rawDatasetHTTP = sc.textFile(s"${args(0)}/DDoS_HTTP.csv")

    sampleDataset(rawDatasetTCP, s"${args(0)}/DDoS_TCP-sampled")
    sampleDataset(rawDatasetUDP, s"${args(0)}/DDoS_UDP-sampled")
    sampleDataset(rawDatasetHTTP, s"${args(0)}/DDoS_HTTP-sampled")
  }

  private def sampleDataset(rdd: RDD[String], saveAs: String): Unit = {
    rdd
      .sample(withReplacement = false, 0.1)
      .coalesce(1)
      .saveAsTextFile(saveAs)
  }
}
