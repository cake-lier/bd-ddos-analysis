package it.unibo.bd
package queries

import org.apache.spark.{ SparkConf, SparkContext }

object WellKnownPorts {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("WellKnownPorts"))
    println(s"Application started at http://localhost:20888/proxy/${sc.applicationId}/\n")

    /*
    val packets =
      sc.textFile(s"${args(0)}/ddos-dataset.csv")
        .map(_.split(","))
        .map(Packet(_))
        .filter(_.isDefined)
        .map(_.get)

    val ports =
      sc.textFile(s"${args(0)}/ports.csv")
        .map(_.split(","))
        .map(PortDescription(_))
        .filter(_.isDefined)
        .map(_.get)
        .map { case PortDescription(port, protocol, description) => (port, protocol) -> description }
        .cache()

    /* Plain JOIN */
    val mostFrequentService =
      packets
        .groupBy(c => c.flowId)
        .map(g => g._2.toSeq.minBy(_.index))
        .map(c => (c.destinationPort, c.protocol))
        .map((_, 1))
        .reduceByKey(_ + _)
        .sortBy(_._2, ascending = false)
        .cache()

    mostFrequentService
      .join(ports)
      .collect()
      .foreach(r => println(s"${r._1}: (Occurrences: ${r._2._1}, Description: ${r._2._2})"))

    println()

    /* Broadcasting ports */
    val broadcastPorts = sc.broadcast(ports.collectAsMap())

    printFormatted(
      mostFrequentService
        .map { case ((port, protocol), count) =>
          broadcastPorts.value.get((port, protocol)).map((port, protocol, count, _))
        },
    )

    println()

    /* Broadcasting packets */
    val broadcastPackets = sc.broadcast(mostFrequentService.collectAsMap())

    printFormatted(
      ports
        .map { case ((port, protocol), description) =>
          broadcastPackets.value.get((port, protocol)).map((port, protocol, _, description))
        },
    )
  }

  def printFormatted(rdd: RDD[Option[(Long, NetworkProtocol, Int, String)]]): Unit =
    rdd
      .filter(_.isDefined)
      .map(_.get)
      .collect()
      .foreach(r => println(s"(${r._1}, ${r._2}): (Occurrences: ${r._3}, Description: ${r._4})"))


     */
  }
}
