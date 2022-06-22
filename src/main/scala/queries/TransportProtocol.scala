package it.unibo.bd
package queries

import com.cibo.evilplot.plot.PieChart
import com.cibo.evilplot.plot.aesthetics.DefaultTheme.defaultTheme
import it.unibo.bd.utils.Record
import org.apache.spark.{ SparkConf, SparkContext }

import java.io.File

object TransportProtocol {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("NetProtocol"))
    println(s"Application started at http://localhost:20888/proxy/${sc.applicationId}/\n")

    val pathTCPDataset = s"${args(0)}/DDoS_TCP.csv"
    val pathUDPDataset = s"${args(0)}/DDoS_UDP.csv"
    val pathHTTPDataset = s"${args(0)}/DDoS_HTTP.csv"
    val dataset = sc.textFile(s"$pathTCPDataset,$pathUDPDataset,$pathHTTPDataset")

    val recordDataset =
      dataset
        .map(_.replace("\"", ""))
        .map(_.split(";"))
        .map(Record(_))
        .filter(_.isDefined)
        .map(_.get)
        .cache()

    val ddosByProtocol = recordDataset.filter(_.isDDoS).map(_.protocol).countByValue()

    val tcpPercentage = ddosByProtocol("tcp") / ddosByProtocol.values.sum.toDouble * 100
    val udpPercentage = ddosByProtocol("udp") / ddosByProtocol.values.sum.toDouble * 100

    val file = new File("images/protocol-chart.png")
    file.createNewFile()
    PieChart(
      Seq(
        f"TCP: ($tcpPercentage%2.2f%%)" -> ddosByProtocol("tcp"),
        f"UDP: ($udpPercentage%2.2f%%)" -> ddosByProtocol("udp"),
      ),
    )
      .rightLegend(labels = Some(Seq("TCP packets", "UDP packets")))
      .render()
      .write(file)
  }
}
