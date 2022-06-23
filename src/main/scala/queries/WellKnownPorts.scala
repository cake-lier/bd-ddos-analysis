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
    println(s"Application started at http://localhost:4040/proxy/${sc.applicationId}/\n")

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

    val chartSequence =
      portsCountByJoin
        .take(2)
        .map { case ((port, protocol), (count, desc)) =>
          (f"$port - $protocol (${count.toDouble / totalPortsCount * 100}%2.2f%%)", count.toDouble, desc)
        }
        .toSeq

    val finalSequence = chartSequence :+ ("other", totalPortsCount - chartSequence.map(_._2).sum, "other")

    val file = new File("images/ports-chart.png")
    file.createNewFile()
    PieChart(finalSequence.map(t => (t._1, t._2)))
      .rightLegend(labels = Some(finalSequence.map(t => s"${t._1} - ${t._3}")))
      .render()
      .write(file)
  }
}
