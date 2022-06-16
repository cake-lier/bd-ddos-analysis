package it.unibo.bd
package queries

import utils.Packet

import org.apache.spark.{ SparkConf, SparkContext }

object Basic {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("Basic"))
    println(s"Application started at http://localhost:20888/proxy/${sc.applicationId}/\n")

    val packets =
      sc.textFile(s"${args(0)}/ddos-dataset.csv")
        .map(_.split(","))
        .map(Packet(_))
        .filter(_.isDefined)
        .map(_.get)

    val totalRows = packets.count()

    println(s"Total rows: $totalRows")

    val totalDDoSRows = packets.filter(_.isDDoS).count()

    println(
      s"Total DDoS rows: $totalDDoSRows",
      s"Percentage of DDoS rows: ${(totalDDoSRows / totalRows.toDouble) * 100}",
    )

    val totalFlows = packets.groupBy(_.flowId).count()

    println(s"Total flows: $totalFlows")

    val totalDDoSFlows =
      packets
        .groupBy(_.flowId)
        .filter(g => g._2.forall(_.isDDoS))
        .count()

    println(
      s"Total DDoS flows: $totalDDoSFlows",
      s"Percentage of DDoS flows: ${(totalDDoSFlows / totalFlows.toDouble) * 100}",
    )
  }
}
