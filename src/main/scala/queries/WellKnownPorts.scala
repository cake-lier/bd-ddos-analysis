package it.unibo.bd
package queries

import com.cibo.evilplot.plot.PieChart
import com.cibo.evilplot.plot.aesthetics.DefaultTheme.defaultTheme
import it.unibo.bd.utils.Record
import org.apache.spark.{ SparkConf, SparkContext }

import java.io.File

object WellKnownPorts {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("WellKnownPorts"))
    println(s"Application started at http://localhost:20888/proxy/${sc.applicationId}/\n")

    val pathTCPDataset = s"${args(0)}/DDoS_TCP.csv"
    val pathUDPDataset = s"${args(0)}/DDoS_UDP.csv"
    val pathHTTPDataset = s"${args(0)}/DDoS_HTTP.csv"
    val dataset = sc.textFile(s"$pathTCPDataset,$pathUDPDataset,$pathHTTPDataset")
    val ports = sc.textFile(s"${args(0)}/ports.csv")

    val recordDataset =
      dataset
        .map(_.replace("\"", ""))
        .map(_.split(";"))
        .map(Record(_))
        .filter(_.isDefined)
        .map(_.get)
        .cache()

    val portsDataset = ports
      .map(_.replace("\"", ""))
      .map(_.split(","))
      .map(a => ((a(0).toLong, a(1).toLowerCase), a(2)))

    val portsCountByJoin = recordDataset
      .filter(_.isDDoS)
      .map(r => ((r.destinationPort, r.protocol), 1))
      .reduceByKey(_ + _)
      .join(portsDataset)
      .sortBy(_._2, ascending = false)

    val totalPortsCount = portsCountByJoin.map(_._2._1).reduce(_ + _).toDouble

    val chartSequence = portsCountByJoin
      .take(2)
      .map { case ((port, protocol), (count, desc)) =>
        (f"$port - $protocol (${count.toDouble / totalPortsCount * 100}%2.2f%%)", count.toDouble, desc)
      }
      .toSeq

    val finalSequence = chartSequence :+ ("other", totalPortsCount - chartSequence.map(_._2).sum, "other")

    val file = new File("/home/nicolas/Documents/uni/lm/bd/bd-ddos-analysis/ports-chart.png")
    file.createNewFile()

    PieChart(finalSequence.map(t => (t._1, t._2)))
      .rightLegend(labels = Some(finalSequence.map(t => s"${t._1} - ${t._3}")))
      .render()
      .write(file)
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
