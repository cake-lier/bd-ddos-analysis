package it.unibo.bd
package utils

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

object Utils {

  implicit class RichRDD[A: ClassTag](self: RDD[A]) {

    def skip(n: Int): RDD[A] = self.zipWithIndex().filter(_._2 >= n).map(_._1)
  }

  implicit class RichSeqRDD[A: ClassTag](self: RDD[Seq[A]]) {

    def toColumns(h: Seq[String]): RDD[(String, Seq[A])] =
      self
        .flatMap(_.zipWithIndex)
        .groupBy(_._2)
        .sortByKey()
        .map { case (i, vs) => (h(i), vs.map(_._1).toSeq) }
  }
}
