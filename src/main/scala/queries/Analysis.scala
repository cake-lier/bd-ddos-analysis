package it.unibo.bd
package queries

import utils.Record

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.storage.StorageLevel

object Analysis {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("Analysis"))
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

    val flowDataDataset =
      recordDataset
        .filter(_.duration > 0.0)
        .map(r =>
          (
            (r.sourceAddress, r.sourcePort, r.destinationAddress, r.destinationPort, r.protocol),
            (r.duration, r.packets, r.bytes),
          ),
        )
        .reduceByKey { case ((duration1, packets1, bytes1), (duration2, packets2, bytes2)) =>
          (duration1 + duration2, packets1 + packets2, bytes1 + bytes2)
        }
        .map { case (k, (duration, packets, bytes)) => (k, (packets / duration, bytes / duration)) }

    val metricDataset =
      recordDataset
        .map(r =>
          (
            (r.sourceAddress, r.sourcePort, r.destinationAddress, r.destinationPort, r.protocol),
            (r.packets, r.bytes, r.rate, if (r.duration > 0.0) r.bytes / r.duration else 0.0, r.isDDoS),
          ),
        )
        .join(flowDataDataset)
        .map {
          case (
                (_, _, destinationAddress, destinationPort, _),
                ((packets, bytes, rate, byteRate, isDDoS), (flowRate, flowByteRate)),
              ) =>
            (
              isDDoS,
              metric(
                destinationPort,
                destinationAddress,
                packets,
                bytes,
                rate,
                byteRate,
                flowRate,
                flowByteRate,
              ) >= 0.01,
            )
        }
        .map { case (actual, predicted) =>
          if (actual == predicted) (if (actual) (1, 0) else (0, 1), (0, 0))
          else ((0, 0), if (!actual) (1, 0) else (0, 1))
        }
        .reduce {
          case (
                ((truePositive1, trueNegative1), (falsePositive1, falseNegative1)),
                ((truePositive2, trueNegative2), (falsePositive2, falseNegative2)),
              ) =>
            (
              (truePositive1 + truePositive2, trueNegative1 + trueNegative2),
              (falsePositive1 + falsePositive2, falseNegative1 + falseNegative2),
            )
        }

    println("|                    | Actually positive | Actually negative |")
    println(f"| Predicted positive | ${metricDataset._1._1.toString}%-17s | ${metricDataset._1._2.toString}%-17s |")
    println(f"| Predicted negative | ${metricDataset._2._2.toString}%-17s | ${metricDataset._2._1.toString}%-17s |")
  }

  def metric(
      destinationPort: Long,
      destinationAddress: String,
      packets: Long,
      bytes: Long,
      rate: Double,
      byteRate: Double,
      flowRate: Double,
      flowByteRate: Double,
  ): Double =
    ((if (destinationPort == 80L) 1 else 0) +
      (if (Set("192.168.100.3", "192.168.100.6", "192.168.100.7")(destinationAddress)) 1 else 0) +
      (1 - math.min(math.abs(packets - 5.909105626202674) / (3 * 3.2784993361685184), 1.0)) +
      (1 - math.min(math.abs(bytes - 529.8851801659653) / (3 * 240.0539442965255), 1.0)) +
      (1 - math.min(math.abs(rate - 0.27486262284753876) / (3 * 0.18712897621757338), 1.0)) +
      (1 - math.min(math.abs(byteRate - 33.86546680087338) / (3 * 17.010164888097375), 1.0)) +
      (1 - math.min(math.abs(flowRate - 0.27403419840566284) / (3 * 0.15597820418003489), 1.0)) +
      (1 - math.min(math.abs(flowByteRate - 26.483191236399332) / (3 * 14.608112880857705), 1.0))) / 8
}
