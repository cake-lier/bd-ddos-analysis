package it.unibo.bd
package queries

import utils.Metric.checkDDoS
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
              checkDDoS(
                destinationPort,
                destinationAddress,
                packets,
                bytes,
                rate,
                byteRate,
                flowRate,
                flowByteRate,
              ),
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
}
