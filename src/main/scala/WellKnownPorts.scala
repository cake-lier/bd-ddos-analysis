package it.unibo.bd

import utils.{ Connection, NetworkProtocol, PortDescription }

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd.RDD

object WellKnownPorts extends App {
  val sc = new SparkContext(new SparkConf().setAppName("WellKnownPorts"))
  println(s"Application started at http://localhost:20888/proxy/${sc.applicationId}/\n")

  val dataset = sc.textFile(s"s3://${args(0)}/ddos-dataset.csv")
  val portDataset = sc.textFile(s"s3://${args(0)}/ports.csv")

  val packets = dataset.map(_.split(",")).map(Connection(_)).filter(_.isDefined).map(_.get)

  val ports = portDataset.map(_.split(",")).map(PortDescription(_)).filter(_.isDefined).map(_.get).map {
    case PortDescription(port, protocol, description) =>
      (port, protocol) -> description
  }

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
  printResult(mostFrequentPorts.join(ports))

  /* With broadcasting ports */
  val broadcastPorts = sc.broadcast(ports.collectAsMap())
  printResult(mostFrequentPorts.map(r => (r._1, (r._2, broadcastPorts.value(r._1)))))

  def printResult(rdd: RDD[((Long, NetworkProtocol), (Int, String))]): Unit =
    rdd
      .collect()
      .foreach(r => s"${r._1}: (Occurrences: ${r._2._1}, Description: ${r._2._2})")

  /* With broadcasting packets */
  val broadcastPackets = sc.broadcast(mostFrequentPorts.collectAsMap())
  printResult(ports.map(r => (r._1, (broadcastPackets.value(r._1), r._2))))
}
