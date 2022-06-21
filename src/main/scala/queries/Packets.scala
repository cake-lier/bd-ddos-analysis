package it.unibo.bd
package queries

import utils.{ DoubleStatistics, Record }
import utils.RichTuples.RichTuple2

import com.cibo.evilplot.plot.{ FunctionPlot, Overlay }
import com.cibo.evilplot.plot.aesthetics.DefaultTheme.defaultTheme
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd.RDD

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
    val ddosDataset = recordDataset.filter(_.isDDoS)
    val legitDataset = recordDataset.filter(!_.isDDoS)
    val extractors: Seq[Record => Long] = Seq(_.packets, _.bytes, _.sourceBytes, _.destinationBytes)

    val ddosResults =
      getStatistics(sc, ddosDataset, extractors)
    val legitResults =
      getStatistics(sc, legitDataset, extractors)

    val ddosRanges =
      ddosResults
        .zip(extractors)
        .map { case (s, e) => countGaussianRange(sc, ddosDataset.map(e), s.mean - 3 * s.stdDev, s.mean + 3 * s.stdDev) }
    val legitRanges =
      legitResults
        .zip(extractors)
        .map { case (s, e) =>
          countGaussianRange(sc, legitDataset.map(e), s.mean - 3 * s.stdDev, s.mean + 3 * s.stdDev)
        }

    Overlay(
      FunctionPlot(gaussian(_, ddosResults.head.mean, ddosResults.head.stdDev)),
    ).title("A bunch of polynomials.")
      .overlayLegend()
      .standard()
      .render()

  }

  def gaussian(x: Double, avg: Double, stdDev: Double): Double = {
    1 / (stdDev * Math.sqrt(2 * Math.PI)) * Math.exp(-0.5 * Math.pow(x - avg, 2) / Math.pow(stdDev, 2))
  }

  def getStatistics(
      sc: SparkContext,
      dataset: RDD[Record],
      extractors: Seq[Record => Long],
  ): Seq[DoubleStatistics] = {
    val results =
      dataset
        .map(r => extractors.map(_.apply(r)).map(v => (v, v, v, 1)))
        .reduce((s1, s2) =>
          s1.zip(s2).map { case ((min1, max1, avg1, count1), (min2, max2, avg2, count2)) =>
            (min1 min min2, max1 max max2, avg1 + avg2, count1 + count2)
          },
        )

    val means = results.map(v => v._3 / v._4.toDouble)
    val meansBroadcast = sc.broadcast(means)

    val stdDevs =
      dataset
        .map(r =>
          extractors.map(_.apply(r)).zip(meansBroadcast.value).map { case (value, mean) => Math.pow(value - mean, 2) },
        )
        .reduce((s1, s2) => s1.zip(s2).map(t => t._1 + t._2))
        .zip(results.map(_._4))
        .map(t => Math.sqrt(t._1 / t._2))

    results.zip(means).zip(stdDevs).map { case (((min, max, _, _), mean), stdDev) =>
      DoubleStatistics(min, max, mean, stdDev)
    }
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
