package it.unibo.bd
package queries

import it.unibo.bd.utils.Record
import org.apache.spark.{ SparkConf, SparkContext }

import scala.util.Try

object TestNewDataset {
//
//  case class Pkt(
//      saddr: String,
//      daddr: String,
//      sport: Long,
//      dport: Long,
//      protocol: String,
//      pkts: Long,
//      stime: String,
//      ltime: String,
//      isDDoS: Boolean,
//  )
//
//  object Pkt {
//
//    def apply(r: Seq[String]): Option[Pkt] = (for {
//      sport <- Try(r(4).toLong)
//      dport <- Try(r(7).toLong)
//      pkts <- Try(r(8).toLong)
//      protocol = r(2)
//      stime = r.head
//      ltime = r(12)
//      saddr = r(3)
//      daddr = r(6)
//      isDDoS <- Try(r(34).toInt)
//    } yield Pkt(saddr, daddr, sport, dport, protocol, pkts, stime, ltime, if (isDDoS == 1) true else false)).toOption
//  }

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("GeoIP"))

    val rawDataset = sc.textFile("/home/nicolas/Downloads/ddos-dataset.csv/ddos-dataset.csv")

    rawDataset
      .map(_.replace("\"", ""))
      .map(_.split(";"))
      .map(Record(_))
      .filter(_.isDefined)
      .map(_.get)
      .coalesce(1)
      .saveAsTextFile("/home/nicolas/Downloads/shrink")
//      .map(p => ((p.saddr, p.daddr, p.sport, p.dport, p.protocol), (p.isDDoS, p.pkts, p.stime, p.ltime)))
//      .filter(!_._2._1)
//      .groupByKey()
//      .filter { case (_, iterable) =>
//        iterable.exists(!_._1) && iterable.exists(_._1)
//      }
//      .map { case (k, iterable) =>
//        val minTime = iterable.map(i => i._3.toDouble).min
//        val maxTime = iterable.map(_._4.toDouble).max
//        (k, maxTime - minTime)
//      }
//      .sortBy(_._2, ascending = false)
//      .take(5)
//      .collect()

//    println(datasetGroupedByFlow.mkString("Array(", ", ", ")"))
  }
}
