package it.unibo.bd

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

object Merge extends App {
  val sc = new SparkContext(new SparkConf().setAppName("Merge"))
  println(s"Application started at http://localhost:20888/proxy/${sc.applicationId}/\n")

  val unbalancedDataset = sc.textFile(s"s3://${args(0)}/unbalaced_20_80_dataset.csv")

  val balancedDataset = sc.textFile(s"s3://${args(0)}/final_dataset.csv")

  implicit class RichRDD[A: ClassTag](self: RDD[A]) {

    def skip(n: Int): RDD[A] = self.zipWithIndex().filter(_._2 >= n).map(_._1)
  }

  val totalDataset = unbalancedDataset ++ balancedDataset.skip(1)

  totalDataset.coalesce(1).saveAsTextFile(s"s3://${args(0)}/ddos-dataset.csv")

  val sampledDataset = totalDataset.sample(withReplacement = false, fraction = 0.001)

  sampledDataset.coalesce(1).saveAsTextFile(s"s3://${args(0)}/ddos-dataset-sample.csv")
}
