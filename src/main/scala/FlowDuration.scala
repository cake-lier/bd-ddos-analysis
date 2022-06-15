package it.unibo.bd

import it.unibo.bd.utils.Connection
import org.apache.spark.rdd.RDD
import org.apache.spark.{ SparkConf, SparkContext }
import scalax.chart.api._

object FlowDuration {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("WellKnownPorts"))
    println(s"Application started at http://localhost:20888/proxy/${sc.applicationId}/\n")

    val dataset = sc.textFile(s"s3://${args(0)}/ddos-dataset.csv")

    val packets = dataset.map(_.split(",")).map(Connection(_)).filter(_.isDefined).map(_.get).cache()

    val ddosResult = getStatisticalData(sc, packets, isDDoS = true)
    val benignResult = getStatisticalData(sc, packets, isDDoS = false)

    println(s"DDoS: Min: ${ddosResult._1} Max: ${ddosResult._2} Avg: ${ddosResult._3} StdDev: ${ddosResult._4}")
    println(
      s"Benign: Min: ${benignResult._1} Max: ${benignResult._2} Avg: ${benignResult._3} StdDev: ${benignResult._4}",
    )

    val x = (BigDecimal(0) to BigDecimal(1.5e9) by BigDecimal(1.5e6)).map(_.toDouble)
    val yDDoS = x.map(c => (c, gaussian(c, ddosResult._3, ddosResult._4))).toXYSeries("DDoS")
    val yBenign = x.map(c => (c, gaussian(c, benignResult._3, benignResult._4))).toXYSeries("Benign")
    val chart = XYLineChart(Seq(yDDoS, yBenign))
    chart.saveAsPNG("/home/hadoop/chart.png")

  }

  def gaussian(x: Double, avg: Double, stdDev: Double): Double = {
    1 / (stdDev * Math.sqrt(2 * Math.PI)) * Math.exp(-0.5 * Math.pow(x - avg, 2) / Math.pow(stdDev, 2))
  }

  /**
   * Return Min, Max, AVG and StdDev
   * @param sc
   * @param dataset
   * @param isDDoS
   * @return
   */
  def getStatisticalData(sc: SparkContext, dataset: RDD[Connection], isDDoS: Boolean): (Long, Long, Double, Double) = {
    val result = dataset
      .groupBy(_.flowId)
      .filter { case (_, groups) =>
        groups.forall(g => g.isDDoS == isDDoS && g.flowDuration > 0 && g.flowDuration == groups.head.flowDuration)
      }
      .map(g => g._2.head.flowDuration)
      .map(f => (f, f, f, 1))
      .reduce { case ((min1, max1, avg1, count1), (min2, max2, avg2, count2)) =>
        (min1 min min2, max1 max max2, avg1 + avg2, count1 + count2)
      }
    val count = result._4
    val avg = result._3 / count.toDouble

    val avgBroadcast = sc.broadcast(avg)

    val variance = dataset.map(_.flowDuration).map(f => Math.pow(f - avgBroadcast.value, 2)).reduce {
      case (acc, value) => acc + value
    }

    val stdDev = Math.sqrt(variance / count)
    (result._1, result._2, avg, stdDev)
  }
}
