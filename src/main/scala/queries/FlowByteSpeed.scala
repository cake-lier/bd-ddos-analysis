package it.unibo.bd
package queries

import utils.Packet

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd.RDD
import scalax.chart.api.{ ChartPNGExporter, RichTuple2s, XYLineChart }
import scalax.chart.module.XYChartFactories.XYBarChart

object FlowByteSpeed {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("FlowDuration"))
    println(s"Application started at http://localhost:20888/proxy/${sc.applicationId}/\n")

    val packets =
      sc.textFile(s"${args(0)}/ddos-dataset.csv")
        .map(_.split(","))
        .map(Packet(_))
        .filter(_.isDefined)
        .map(_.get)
        .groupBy(_.flowId)
        .cache()

    val benignFlow =
      packets
        .filter { case (_, groups) =>
          groups.forall(g =>
            !g.isDDoS
              && g.byteSpeed > 0
              && g.byteSpeed < Double.PositiveInfinity
              && g.byteSpeed != Double.NaN
              && g.byteSpeed == groups.head.byteSpeed,
          )
        }
        .map(g => g._2.head.byteSpeed)

    val ddosFlow =
      packets
        .filter { case (_, groups) =>
          groups.forall(g =>
            g.isDDoS
              && g.byteSpeed > 0
              && g.byteSpeed < Double.PositiveInfinity
              && g.byteSpeed != Double.NaN
              && g.byteSpeed == groups.head.byteSpeed,
          )
        }
        .map(g => g._2.head.byteSpeed)

    val ddosResult = getStatisticalData(sc, ddosFlow)
    val benignResult = getStatisticalData(sc, benignFlow)

    println(s"DDoS: Min: ${ddosResult._1} Max: ${ddosResult._2} Avg: ${ddosResult._3} StdDev: ${ddosResult._4}")
    println(
      s"Benign: Min: ${benignResult._1} Max: ${benignResult._2} Avg: ${benignResult._3} StdDev: ${benignResult._4}",
    )

    val avgDDoS = ddosResult._3
    val stdDevDDoS = ddosResult._4
    val avgBenign = benignResult._3
    val stdDevBenign = benignResult._4

    val (inRangeDDoS, outRangeDDoS) =
      countGaussianRange(sc, ddosFlow, avgDDoS - 3 * stdDevDDoS, avgDDoS + 3 * stdDevDDoS)
    val (inRangeBenign, outRangeBenign) =
      countGaussianRange(sc, benignFlow, avgBenign - 3 * stdDevBenign, avgBenign + 3 * stdDevBenign)

    println(
      s"DDoS in range: $inRangeDDoS, out of range: $outRangeDDoS, " +
        s"percentage in range: ${inRangeDDoS / (inRangeDDoS + outRangeDDoS) * 100.0}",
    )
    println(
      s"DDoS in range: $inRangeBenign, out of range: $outRangeBenign, " +
        s"percentage in range: ${inRangeBenign / (inRangeBenign + outRangeBenign) * 100.0}",
    )

    val (ddosBins, ddosCounts) = ddosFlow.histogram(100)
    val (benignBins, benignCounts) = benignFlow.histogram(100)

    val histogramDDoS =
      ddosBins.zip(ddosCounts.map(_.toDouble).map(_ / ddosCounts.sum.toDouble)).toSeq.toXYSeries("Histogram DDoS")
    val histogramBenign = benignBins
      .zip(benignCounts.map(_.toDouble).map(_ / benignCounts.sum.toDouble))
      .toSeq
      .toXYSeries("Histogram Benign")

    val x = (BigDecimal(0) to BigDecimal(1e8) by BigDecimal(1e6)).map(_.toDouble)

    val gaussianDDoS = x.map(c => (c, gaussian(c, ddosResult._3, ddosResult._4))).toXYSeries("DDoS")
    val gaussianBenign = x.map(c => (c, gaussian(c, benignResult._3, benignResult._4))).toXYSeries("Benign")

    val gaussianDDoSCorrectnessChart = XYBarChart(Seq(histogramDDoS))
    gaussianDDoSCorrectnessChart.saveAsPNG("ddos_correctness.png")
    val gaussianBenignCorrectnessChart = XYBarChart(Seq(histogramBenign))
    gaussianBenignCorrectnessChart.saveAsPNG("benign_correctness.png")

    val chart = XYLineChart(Seq(gaussianDDoS, gaussianBenign))
    chart.saveAsPNG("chart.png")
  }

  def gaussian(x: Double, avg: Double, stdDev: Double): Double = {
    1 / (stdDev * Math.sqrt(2 * Math.PI)) * Math.exp(-0.5 * Math.pow(x - avg, 2) / Math.pow(stdDev, 2))
  }

  /**
   * Return Min, Max, Avg and StdDev
   *
   * @param sc
   * @param dataset
   * @param isDDoS
   * @return
   */
  def getStatisticalData(sc: SparkContext, dataset: RDD[Double]): (Double, Double, Double, Double) = {
    val result =
      dataset
        .map(s => (s, s, s, 1))
        .reduce { case ((min1, max1, avg1, count1), (min2, max2, avg2, count2)) =>
          (min1 min min2, max1 max max2, avg1 + avg2, count1 + count2)
        }
    val count = result._4
    val avg = result._3 / count.toDouble

    val avgBroadcast = sc.broadcast(avg)

    val variance =
      dataset
        .map(f => Math.pow(f - avgBroadcast.value, 2))
        .reduce(_ + _)

    val stdDev = Math.sqrt(variance / count)
    (result._1, result._2, avg, stdDev)
  }

  /**
   * Return the number inside the range and outside.
   *
   * @return
   */
  def countGaussianRange(
      sc: SparkContext,
      dataset: RDD[Double],
      lower: Double,
      upper: Double,
  ): (Int, Int) = {
    val upperBroadcast = sc.broadcast(upper)
    val lowerBroadcast = sc.broadcast(lower)
    dataset
      .map(f => if (f > lowerBroadcast.value && f < upperBroadcast.value) (1, 0) else (0, 1))
      .reduce { case ((inRange1, outRange1), (inRange2, outRange2)) =>
        (inRange1 + inRange2, outRange1 + outRange2)
      }
  }
}