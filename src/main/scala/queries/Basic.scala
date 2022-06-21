package it.unibo.bd
package queries

import utils.Record

import com.cibo.evilplot.plot.PieChart
import com.cibo.evilplot.plot.aesthetics.DefaultTheme.defaultTheme
import org.apache.spark.{ SparkConf, SparkContext }
import utils.RichTuples.RichTuple2

import java.io.File

object Basic {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("Basic"))
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

    val recordDatasetSize = recordDataset.count()
    val ddosCount =
      recordDataset
        .map(r => if (r.isDDoS) (1, 0) else (0, 1))
        .reduce(_ + _)
    val ddosPercentage = ddosCount._1 / recordDatasetSize.toDouble * 100
    val legitPercentage = ddosCount._2 / recordDatasetSize.toDouble * 100

    val file = new File("total_pie.png")
    file.createNewFile()
    PieChart(
      Seq(
        f"DDoS traffic ($ddosPercentage%2.4f%%)" -> ddosCount._1.toDouble,
        f"Legit traffic ($legitPercentage%2.4f%%)" -> ddosCount._2.toDouble,
      ),
    )
      .rightLegend(labels = Some(Seq("DDoS traffic", "Legit traffic")))
      .render()
      .write(file)
  }
}
