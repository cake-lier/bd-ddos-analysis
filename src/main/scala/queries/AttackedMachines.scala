package it.unibo.bd
package queries

import utils.Record

import com.cibo.evilplot.plot.BarChart
import com.cibo.evilplot.plot.aesthetics.DefaultTheme.defaultTheme
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.storage.StorageLevel

import java.io.File

object AttackedMachines {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("AttackedMachines"))
    println(s"Application started at http://localhost:4040/proxy/${sc.applicationId}/\n")

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
        .persist(StorageLevel.MEMORY_AND_DISK)

    val ddosTrafficByIP =
      recordDataset
        .filter(_.isDDoS)
        .map(r => (r.destinationAddress, r.bytes))
        .reduceByKey(_ + _)
        .map { case (ip, traffic) => (ip, traffic / 1024.0) }
        .sortBy(_._2, ascending = false)
        .take(5)
        .toMap

    val trafficByIP =
      recordDataset
        .map(r => (r.destinationAddress, r.bytes))
        .reduceByKey(_ + _)
        .map { case (ip, traffic) => (ip, traffic / 1024.0) }
        .collect()
        .toMap

    val totalImportantTraffic = trafficByIP.filterKeys(ddosTrafficByIP.keySet(_))
    val totalImportantTrafficSorted = totalImportantTraffic.toSeq.sortBy(_._1)
    val ddosTrafficSorted = ddosTrafficByIP.toSeq.sortBy(_._1)

    val file = new File("images/ddos-traffic.png")
    file.createNewFile()
    BarChart
      .clustered(
        totalImportantTrafficSorted.map(_._2).zip(ddosTrafficSorted.map(_._2)).map(v => Seq(v._1, v._2)),
        labels = totalImportantTrafficSorted.map(_._1),
      )
      .standard(totalImportantTrafficSorted.map(_._1))
      .xLabel("IP")
      .yLabel("KB")
      .rightLegend(labels = Some(Seq("Total", "DDoS")))
      .render()
      .write(file)
  }
}
