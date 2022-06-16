package it.unibo.bd
package queries

import org.apache.spark.{ SparkConf, SparkContext }

import scala.util.Try

object TestNewDataset {
  case class Pkt(saddr: String, daddr: String, sport: Long, dport: Long, pkts: Long, stime: String)

  object Pkt {

    def apply(r: Seq[String]): Option[Pkt] = (for {
      sport <- Try(r(4).toLong)
      dport <- Try(r(7).toLong)
      pkts <- Try(r(8).toLong)
      stime = r.head
      saddr = r(3)
      daddr = r(6)
    } yield Pkt(saddr, daddr, sport, dport, pkts, stime)).toOption
  }

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("GeoIP"))

    val rawDataset = sc.textFile("/home/nicolas/Downloads/DDoS_TCP.csv")

    val datasetGroupedByFlow = rawDataset
      .map(_.replace("\"", ""))
      .map(_.split(";"))
      .map(Pkt(_))
      .filter(_.isDefined)
      .map(_.get)
      .map(p => ((p.saddr, p.daddr, p.sport, p.dport, p.pkts), p.stime))
      .groupByKey()
      .map { case (k, iterable) =>
        (k, iterable.size)
      }
      // .take(20)
      .collect()

    println(datasetGroupedByFlow.mkString("Array(", ", ", ")"))
  }
}
