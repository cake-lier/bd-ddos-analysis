package it.unibo.bd

import it.unibo.bd.utils.{ Connection, PortDescription }
import org.apache.spark.{ SparkConf, SparkContext }

object WellKnownPorts extends App {
  val sc = new SparkContext(new SparkConf().setAppName("WellKnownPorts"))
  println(s"Application started at http://localhost:20888/proxy/${sc.applicationId}/\n")

  val dataset = sc.textFile(s"s3://${args(0)}/ddos-dataset.csv")
  val portDataset = sc.textFile(s"s3://${args(0)}/ports.csv")

  val packets = dataset.map(_.split(",")).map(Connection(_)).filter(_.isDefined).map(_.get)
  val ports = portDataset.map(_.split(",")).map(PortDescription(_)).filter(_.isDefined).map(_.get)

  /* Plain JOIN */
  val joinedDataset = packets
    .groupBy(c => (c.flowId, c.index))
    .sortByKey()
    .map(c => (c._2.head.destinationPort, c._2.head.protocol))
    .map((_, 1))
    .reduceByKey(_ + _)
    .join(ports.map { case PortDescription(port, protocol, description) =>
      (port, protocol) -> description
    })
  /* ---------- */
}
