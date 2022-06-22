package it.unibo.bd
package queries

import utils.{ Gaussian, Quartiles, Record }
import utils.RichTuples.RichTuple2

import com.cibo.evilplot.colors.HTMLNamedColors
import com.cibo.evilplot.numeric.Bounds
import com.cibo.evilplot.plot.{ Facets, FunctionPlot, Overlay }
import com.cibo.evilplot.plot.aesthetics.DefaultTheme.defaultTheme
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd.RDD
import spire.ClassTag

import java.io.File
import scala.math.Numeric.Implicits.infixNumericOps

object Packets {

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
    val ddosDataset = recordDataset.filter(_.isDDoS).cache()
    val legitDataset = recordDataset.filter(!_.isDDoS).cache()

    val packetsDDoS = ddosDataset.map(_.packets)
    val packetsLegit = legitDataset.map(_.packets)
    val bytesDDoS = ddosDataset.map(_.bytes)
    val bytesLegit = legitDataset.map(_.bytes)
    val rateDDoS = ddosDataset.map(_.rate)
    val rateLegit = legitDataset.map(_.rate)
    val bytesRateDDoS = ddosDataset.map(r => r.bytes / r.duration)
    val bytesRateLegit = legitDataset.map(r => r.bytes / r.duration)

    val packetQuartileDDoS = getStatistics(packetsDDoS)
    val packetQuartileLegit = getStatistics(packetsLegit)
    val bytesQuartileDDoS = getStatistics(bytesDDoS)
    val bytesQuartileLegit = getStatistics(bytesLegit)
    val rateQuartileDDoS = getStatistics(rateDDoS)
    val rateQuartileLegit = getStatistics(rateLegit)
    val bytesRateQuartileDDoS = getStatistics(bytesRateDDoS)
    val bytesRateQuartileLegit = getStatistics(bytesRateLegit)

    val packetDDoSGaussian = getGaussian(sc, cleanByIQR(sc, packetsDDoS, packetQuartileDDoS))
    val packetLegitGaussian = getGaussian(sc, cleanByIQR(sc, packetsLegit, packetQuartileLegit))
    val bytesQuartileDDoSGaussian = getGaussian(sc, cleanByIQR(sc, bytesDDoS, bytesQuartileDDoS))
    val bytesQuartileLegitGaussian = getGaussian(sc, cleanByIQR(sc, bytesLegit, bytesQuartileLegit))
    val rateDDoSGaussian = getGaussian(sc, cleanByIQR(sc, rateDDoS, rateQuartileDDoS))
    val rateLegitGaussian = getGaussian(sc, cleanByIQR(sc, rateLegit, rateQuartileLegit))
    val bytesRateDDoSGaussian = getGaussian(sc, cleanByIQR(sc, bytesRateDDoS, bytesRateQuartileDDoS))
    val bytesRateLegitGaussian = getGaussian(sc, cleanByIQR(sc, bytesRateLegit, bytesRateQuartileLegit))

    showPlot(
      packetDDoSGaussian,
      packetLegitGaussian,
      packetQuartileDDoS,
      packetQuartileLegit,
      "packets",
      "packets",
    )
    showPlot(
      bytesQuartileDDoSGaussian,
      bytesQuartileLegitGaussian,
      bytesQuartileDDoS,
      bytesQuartileLegit,
      "bytes",
      "bytes",
    )
    showPlot(
      rateDDoSGaussian,
      rateLegitGaussian,
      rateQuartileDDoS,
      rateQuartileLegit,
      "rates",
      "rates",
    )
    showPlot(
      bytesRateDDoSGaussian,
      bytesRateLegitGaussian,
      bytesRateQuartileDDoS,
      bytesRateQuartileLegit,
      "bytesRate",
      "bytesRate",
    )
  }

  def showPlot[T: Numeric](
      gaussianDDoS: Gaussian,
      gaussianLegit: Gaussian,
      quartilesDDoS: Quartiles[T],
      quartilesLegit: Quartiles[T],
      filename: String,
      variableName: String,
  ): Unit = {
    val file = new File(s"images/$filename.png")
    file.createNewFile()

    println(s"""
      | DDoS 
      | \tMin: ${quartilesDDoS.min}
      | \tlower: ${quartilesDDoS.firstQuartile
        .toDouble() - 1.5 * (quartilesDDoS.thirdQuartile - quartilesDDoS.firstQuartile).toDouble()}
      | \tFirst Quartile: ${quartilesDDoS.firstQuartile}
      | \tMedian: ${quartilesDDoS.secondQuartile}
      | \tThird Quartile: ${quartilesDDoS.thirdQuartile}
      | \tUpper: ${quartilesDDoS.thirdQuartile
        .toDouble() + 1.5 * (quartilesDDoS.thirdQuartile - quartilesDDoS.firstQuartile).toDouble()}
      | \tMax: ${quartilesDDoS.max}
      """.stripMargin)

    println(s"""
               | Legit 
               | \tMin: ${quartilesLegit.min}
               | \tlower: ${quartilesLegit.firstQuartile
        .toDouble() - 1.5 * (quartilesLegit.thirdQuartile - quartilesLegit.firstQuartile).toDouble()}
               | \tFirst Quartile: ${quartilesLegit.firstQuartile}
               | \tMedian: ${quartilesLegit.secondQuartile}
               | \tThird Quartile: ${quartilesLegit.thirdQuartile}
               | \tUpper: ${quartilesLegit.thirdQuartile
        .toDouble() + 1.5 * (quartilesLegit.thirdQuartile - quartilesLegit.firstQuartile).toDouble()}
               | \tMax: ${quartilesLegit.max}
      """.stripMargin)

    Facets(
      Seq(
        Seq(
          FunctionPlot.series(
            gaussian(gaussianDDoS.mean, gaussianDDoS.stdDev),
            "DDoS",
            HTMLNamedColors.dodgerBlue,
            Some(Bounds(0, gaussianDDoS.mean + 3 * gaussianDDoS.stdDev)),
          ),
          FunctionPlot.series(
            gaussian(gaussianLegit.mean, gaussianLegit.stdDev),
            "Legit",
            HTMLNamedColors.orange,
            Some(Bounds(0, gaussianLegit.mean + 3 * gaussianLegit.stdDev)),
          ),
        ),
      ),
    )
      .title(s"Difference in distribution between DDoS and legit $variableName")
      .overlayLegend()
      .standard()
      .render()
      .write(file)
  }

  def gaussian(avg: Double, stdDev: Double)(x: Double): Double =
    1 / (stdDev * math.sqrt(2 * math.Pi)) * math.exp(-0.5 * math.pow(x - avg, 2) / math.pow(stdDev, 2))

  def getStatistics[T: Numeric: Ordering: ClassTag](
      dataset: RDD[T],
  ): Quartiles[T] = {
    val size = dataset.count()
    val quartiles = dataset
      .sortBy(v => v)
      .zipWithIndex()
      .filter { case (_, index) =>
        index == 0 || index == size / 4 || index == size / 2 || index == size * 3 / 4 || index == size - 1
      }
      .collect()

    quartiles.sortBy(_._2).map(_._1) match {
      case Array(min, first, second, third, max) => Quartiles(min, first, second, third, max)
    }
  }

  def getGaussian[T: Numeric](sc: SparkContext, rdd: RDD[T]): Gaussian = {
    val (sum, count) = rdd.map(r => (r, 1)).reduce(_ + _)
    val mean = sum.toDouble() / count
    val meanBroadcast = sc.broadcast(mean)

    val stdDev = math.sqrt(rdd.map(r => math.pow(r.toDouble() - meanBroadcast.value, 2)).sum() / count)
    Gaussian(mean, stdDev)
  }

  def cleanByIQR[T: Numeric: Ordering](sc: SparkContext, rdd: RDD[T], quartiles: Quartiles[T]): RDD[T] = {
    val rangeBroadcast = sc.broadcast(
      (
        quartiles.firstQuartile.toDouble() - 1.5 * (quartiles.thirdQuartile - quartiles.firstQuartile).toDouble(),
        quartiles.thirdQuartile.toDouble() + 1.5 * (quartiles.thirdQuartile - quartiles.firstQuartile).toDouble(),
      ),
    )
    rdd.filter(r => r.toDouble() > rangeBroadcast.value._1 && r.toDouble() < rangeBroadcast.value._2)
  }

  /**
   * Return the number inside the range and outside.
   * @return
   */
  def countGaussianRange(
      sc: SparkContext,
      dataset: RDD[Long],
      lower: Double,
      upper: Double,
  ): (Int, Int) = {
    val upperBroadcast = sc.broadcast(upper)
    val lowerBroadcast = sc.broadcast(lower)
    dataset
      .map(f => if (f > lowerBroadcast.value && f < upperBroadcast.value) (1, 0) else (0, 1))
      .reduce(_ + _)
  }
}
