package it.unibo.bd

import it.unibo.bd.utils.Utils.RichRDD
import org.apache.spark.{ SparkConf, SparkContext }

object Merge {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("Merge"))
    println(s"Application started at http://localhost:20888/proxy/${sc.applicationId}/\n")

    val httpDataset = sc.textFile("/home/nicolas/Downloads/DDoS_HTTP.csv")
    val tcpDataset = sc.textFile("/home/nicolas/Downloads/DDoS_TCP.csv")
    val udpDataset = sc.textFile("/home/nicolas/Downloads/DDoS_UDP.csv")

    val totalDataset = httpDataset.skip(1) ++ tcpDataset.skip(1) ++ udpDataset.skip(1)
    totalDataset.coalesce(1).saveAsTextFile("/home/nicolas/Downloads/ddos-dataset.csv")

    val sampledDataset = totalDataset.sample(withReplacement = false, 0.001)
    sampledDataset.coalesce(1).saveAsTextFile("/home/nicolas/Downloads/ddos-dataset-sampled.csv")
  }
}
