package it.unibo.bd

import utils.{ Connection, TCP, UDP }

import org.apache.spark.{ SparkConf, SparkContext }

object NetProtocol extends App {
  val sc = new SparkContext(new SparkConf().setAppName("WellKnownPorts"))
  println(s"Application started at http://localhost:20888/proxy/${sc.applicationId}/\n")

  val dataset = sc.textFile(s"s3://${args(0)}/ddos-dataset.csv")

  val packets = dataset.map(_.split(",")).map(Connection(_)).filter(_.isDefined).map(_.get)

  packets
    .map(_.protocol)
    .countByValue()
    .foreach(println(_))

  packets
    .filter(_.isDDoS)
    .map(_.protocol)
    .countByValue()
    .foreach(println(_))

  val values = packets
    .map(c =>
      (
        if (c.protocol == TCP) 1 else 0,
        if (c.protocol == UDP) 1 else 0,
        if (c.protocol == TCP && c.isDDoS) 1 else 0,
        if (c.protocol == UDP && c.isDDoS) 1 else 0,
      ),
    )
    .reduce {
      case (
            (tcpCount1, udpCount1, tcpDDoSCount1, udpDDoSCount1),
            (tcpCount2, udpCount2, tcpDDoSCount2, udpDDoSCount2),
          ) =>
        (tcpCount1 + tcpCount2, udpCount1 + udpCount2, tcpDDoSCount1 + tcpDDoSCount2, udpDDoSCount1 + udpDDoSCount2)
    }
  println(values._2 / values._1.toDouble)
  println(values._4 / values._3.toDouble)
}
