package it.unibo.bd

import utils.{ Connection, NetworkProtocol, PortDescription }

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd.RDD

object WellKnownPorts {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("WellKnownPorts"))
    println(s"Application started at http://localhost:20888/proxy/${sc.applicationId}/\n")

    val dataset = sc.textFile(s"s3://${args(0)}/ddos-dataset.csv")
    val portDataset = sc.textFile(s"s3://${args(0)}/ports.csv")

    val packets = dataset.map(_.split(",")).map(Connection(_)).filter(_.isDefined).map(_.get).cache()

    val ports = portDataset
      .map(_.split(","))
      .map(PortDescription(_))
      .filter(_.isDefined)
      .map(_.get)
      .map { case PortDescription(port, protocol, description) =>
        (port, protocol) -> description
      }
      .cache()

    /* Plain JOIN */
    val mostFrequentPorts =
      packets
        .groupBy(c => (c.flowId, c.index))
        .sortByKey()
        .map(c => (c._2.head.destinationPort, c._2.head.protocol))
        .map((_, 1))
        .reduceByKey(_ + _)
        .sortBy(_._2, ascending = false)
        .cache()

    mostFrequentPorts
      .join(ports)
      .collect()
      .foreach(r => println(s"${r._1}: (Occurrences: ${r._2._1}, Description: ${r._2._2})"))

    println()

    /* With broadcasting ports */
    val broadcastPorts = sc.broadcast(ports.collectAsMap())

    mostFrequentPorts.map { case ((port, protocol), count) =>
      (port, protocol, count, broadcastPorts.value.get((port, protocol)))
    }.filter { case (_, _, _, description) =>
      description.isDefined
    }.map { case (port, protocol, count, description) =>
      (port, protocol, count, description.get)
    }
      .collect()
      .foreach(r => println(s"(${r._1}, ${r._2}): (Occurrences: ${r._3}, Description: ${r._4})"))

    println()

    /* With broadcasting packets */
    val broadcastPackets = sc.broadcast(mostFrequentPorts.collectAsMap())
    printResult(ports.map(r => (r._1, (broadcastPackets.value(r._1), r._2))))
  }

  def printResult(rdd: RDD[((Long, NetworkProtocol), (Int, String))]): Unit =
    rdd
      .collect()
      .foreach(r => println(r))
}
