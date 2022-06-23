package it.unibo.bd
package queries

import utils.Functions.gaussian
import utils.Record
import utils.RichTuples.{ RichTuple4, RichTuple6 }

import com.cibo.evilplot.colors.{ HSLA, HTMLNamedColors }
import com.cibo.evilplot.numeric.Bounds
import com.cibo.evilplot.plot.FunctionPlot
import com.cibo.evilplot.plot.aesthetics.DefaultTheme.defaultTheme
import org.apache.spark.{ SparkConf, SparkContext }

import java.io.File

object Flows {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("Flows"))
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

    val flowsDataset =
      recordDataset
        .map(r =>
          (
            (r.sourceAddress, r.sourcePort, r.destinationAddress, r.destinationPort, r.protocol),
            (r.isDDoS, r.duration, r.packets, r.bytes),
          ),
        )
        .reduceByKey { case ((isDDoS1, duration1, packets1, bytes1), (isDDoS2, duration2, packets2, bytes2)) =>
          (isDDoS1 || isDDoS2, duration1 + duration2, packets1 + packets2, bytes1 + bytes2)
        }
        .map { case (_, (isDDoS, duration, packets, bytes)) => (isDDoS, packets / duration, bytes / duration) }
        .cache()

    val (ddosPacketsRateSum, ddosBytesRateSum, ddosCount, legitPacketsRateSum, legitBytesRateSum, legitCount) =
      flowsDataset
        .map { case (isDDoS, packetsRate, bytesRate) =>
          if (isDDoS) (packetsRate, bytesRate, 1, 0, 0, 0) else (0, 0, 0, packetsRate, bytesRate, 1)
        }
        .reduce(_ + _)

    val ddosPacketsRateMean = ddosPacketsRateSum / ddosCount
    val ddosBytesRateMean = ddosBytesRateSum / ddosCount
    val legitPacketsRateMean = legitPacketsRateSum / legitCount
    val legitBytesRateMean = legitBytesRateSum / legitCount

    val ddosPacketsRateMeanBroadcast = sc.broadcast(ddosPacketsRateMean)
    val ddosBytesRateMeanBroadcast = sc.broadcast(ddosBytesRateMean)
    val legitPacketsRateMeanBroadcast = sc.broadcast(legitPacketsRateMean)
    val legitBytesRateMeanBroadcast = sc.broadcast(legitBytesRateMean)

    val (ddosPacketsRateDiff, ddosBytesRateDiff, legitPacketsRateDiff, legitBytesRateDiff) =
      flowsDataset
        .map { case (isDDoS, packetsRate, bytesRate) =>
          if (isDDoS)
            (
              math.pow(packetsRate - ddosPacketsRateMeanBroadcast.value, 2),
              math.pow(bytesRate - ddosBytesRateMeanBroadcast.value, 2),
              0,
              0,
            )
          else
            (
              0,
              0,
              math.pow(packetsRate - legitPacketsRateMeanBroadcast.value, 2),
              math.pow(bytesRate - legitBytesRateMeanBroadcast.value, 2),
            )
        }
        .reduce(_ + _)

    val ddosPacketsRateStdDev = math.sqrt(ddosPacketsRateDiff / ddosCount)
    val ddosBytesRateStdDev = math.sqrt(ddosBytesRateDiff / ddosCount)
    val legitPacketsRateStdDev = math.sqrt(legitPacketsRateDiff / legitCount)
    val legitBytesRateStdDev = math.sqrt(legitBytesRateDiff / legitCount)

    showPlot(
      ddosPacketsRateMean,
      ddosPacketsRateStdDev,
      "ddos-flow-packets-rate",
      "DDoS",
      "packets rate",
      HTMLNamedColors.dodgerBlue,
    )
    showPlot(
      ddosBytesRateMean,
      ddosBytesRateStdDev,
      "ddos-flow-bytes-rate",
      "DDoS",
      "bytes rate",
      HTMLNamedColors.dodgerBlue,
    )
    showPlot(
      legitPacketsRateMean,
      legitPacketsRateStdDev,
      "legit-flow-packets-rate",
      "Legit",
      "packets rate",
      HTMLNamedColors.orange,
    )
    showPlot(
      legitBytesRateMean,
      legitBytesRateStdDev,
      "legit-flow-bytes-rate",
      "Legit",
      "bytes rate",
      HTMLNamedColors.orange,
    )
  }

  def showPlot(
      mean: Double,
      stdDev: Double,
      filename: String,
      categoryName: String,
      variableName: String,
      color: HSLA,
  ): Unit = {
    val file = new File(s"images/$filename.png")
    file.createNewFile()

    FunctionPlot
      .series(
        gaussian(mean, stdDev),
        categoryName,
        color,
        Some(Bounds(0, mean + 3 * stdDev)),
      )
      .title(s"$categoryName $variableName distribution")
      .overlayLegend()
      .standard()
      .render()
      .write(file)
  }
}
