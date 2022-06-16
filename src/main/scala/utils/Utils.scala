package it.unibo.bd
package utils

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

object Utils {

  implicit class RichRDD[A: ClassTag](self: RDD[A]) {

    def skip(n: Int): RDD[A] = self.zipWithIndex().filter(_._2 >= n).map(_._1)
  }
}
