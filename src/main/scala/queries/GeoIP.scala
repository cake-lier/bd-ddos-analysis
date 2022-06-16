package it.unibo.bd
package queries

import it.unibo.bd.utils.Packet
import org.apache.spark.{ SparkConf, SparkContext }

object GeoIP {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("GeoIP"))
    println(s"Application started at http://localhost:20888/proxy/${sc.applicationId}/\n")

    val ipDataset = sc.textFile(s"${args(0)}/ip2location.csv").cache()
    val dataset = sc
      .textFile(s"${args(0)}/ddos-dataset.csv")
      .map(_.split(","))
      .map(Packet(_))
      .filter(_.isDefined)
      .map(_.get)
      .filter(_.isDDoS)
      .groupBy(_.flowId)
      .map { case (_, iterable) => iterable.toSeq.minBy(_.index) }
      .cache()

    val allIps = ipDataset
      .map(row => row.split(","))
      .map(row => (row(0), row(1), row(2), row(3)))
      .map { case (sIp, eIp, cc, desc) =>
        (sIp.replace("\"", ""), eIp.replace("\"", ""), cc.replace("\"", ""), desc.replace("\"", ""))
      }
      .filter { case (_, _, cc, desc) => cc != "-" || desc != "-" }
      .map { case (startIP, endIp, countryCode, _) =>
        (startIP.toLong, endIp.toLong, countryCode)
      }
      .sortBy(r => r._1)

    val a = dataset
      .map(p => p.destinationIP.getAddress.foldLeft(0L)((acc, v) => (acc << 8) + v).toDouble)
      .histogram(allIps.map(_._1.toDouble).collect() :+ allIps.max()._2.toDouble)

    val result = allIps.map(_._3).collect().zip(a).filter(p => p._2 != 0)

    val histogram = sc.parallelize(result)
    val queryResult = histogram.reduceByKey(_ + _).sortBy(p => p._2, ascending = false).collect()

    println(queryResult.mkString("Array(", ", ", ")"))
  }
}
