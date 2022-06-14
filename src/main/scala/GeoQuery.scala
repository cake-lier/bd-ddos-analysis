package it.unibo.bd

import it.unibo.bd.utils.Utils.RichRDD
import org.apache.spark.{ SparkConf, SparkContext }

object GeoQuery extends App {
  if (args.length < 1) throw new IllegalArgumentException("Dataset path is required")

  val sc = new SparkContext(new SparkConf().setAppName("GeoQuery"))
  println(s"Application started at http://localhost:20888/proxy/${sc.applicationId}/\n")

  val datasetPath = args(1)
  val rawDataset = sc.textFile(datasetPath)

  val dataset = rawDataset.skip(1).map(_.split(",").toSeq)
  val header = rawDataset.first().split(",").tail.toSeq

}
