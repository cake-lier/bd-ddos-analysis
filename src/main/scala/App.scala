package it.unibo.bd

import utils.LiveRecord
import utils.Metric.{ checkDDoS, metric }

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.spark.SparkContext

import scala.io.Source

object App {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("App").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val stream = getClass.getResourceAsStream("/aws_credentials.txt")
    val lines = Source.fromInputStream(stream).getLines.toList
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.fast.upload", "true")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.fast.upload.buffer", "bytebuffer")
    spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", lines.head)
    spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", lines(1))
    app(spark.sparkContext, args(1), args(2).toInt, args(3))
  }

  def app(sc: SparkContext, host: String, port: Int, path: String): Unit = {
    def updateFunction(
        newValues: Seq[(Long, Long, Double, Double)],
        oldValue: Option[(Long, Long, Long, Long, Double, Double, Double)],
    ): Option[(Long, Long, Long, Long, Double, Double, Double)] = {
      val value = oldValue.getOrElse(0L, 0L, 0L, 0L, 0.0, 0.0, 0.0)
      val packets = newValues.lastOption.map(_._1).getOrElse(0L)
      val totalPackets = value._2 + newValues.map(_._1).sum
      val bytes = newValues.lastOption.map(_._2).getOrElse(0L)
      val totalBytes = value._4 + newValues.map(_._2).sum
      val duration = newValues.lastOption.map(_._3).getOrElse(0.0)
      val totalDuration = value._6 + newValues.map(_._3).sum
      val rate = newValues.lastOption.map(_._4).getOrElse(0.0)
      Some((packets, totalPackets, bytes, totalBytes, duration, totalDuration, rate))
    }

    def functionToCreateContext(): StreamingContext = {
      val newSsc = new StreamingContext(sc, Seconds(1))
      newSsc
        .socketTextStream(host, port, StorageLevel.MEMORY_AND_DISK_SER)
        .map(_.replace("\"", ""))
        .map(_.split(";"))
        .map(LiveRecord(_))
        .filter(_.isDefined)
        .map(_.get)
        .map(r =>
          (
            (r.sourceAddress, r.destinationAddress, r.sourcePort, r.destinationPort, r.protocol),
            (r.packets, r.bytes, r.duration, r.rate),
          ),
        )
        .updateStateByKey(updateFunction)
        .map {
          case (
                (sourceAddress, destinationAddress, sourcePort, destinationPort, protocol),
                (packets, totalPackets, bytes, totalBytes, duration, totalDuration, rate),
              ) =>
            (
              (sourceAddress, destinationAddress, sourcePort, destinationPort, protocol),
              (
                metric(
                  destinationPort,
                  destinationAddress,
                  packets,
                  bytes,
                  rate,
                  if (duration > 0.0) bytes / duration else 0.0,
                  if (totalDuration > 0.0) totalPackets / totalDuration else 0.0,
                  if (totalDuration > 0.0) totalBytes / totalDuration else 0.0,
                ),
                checkDDoS(
                  destinationPort,
                  destinationAddress,
                  packets,
                  bytes,
                  rate,
                  if (duration > 0.0) bytes / duration else 0.0,
                  if (totalDuration > 0.0) totalPackets / totalDuration else 0.0,
                  if (totalDuration > 0.0) totalBytes / totalDuration else 0.0,
                ),
              ),
            )
        }
        .print()
      newSsc.checkpoint(path)
      newSsc
    }

    val ssc = StreamingContext.getOrCreate(path, functionToCreateContext)
    ssc.start()
    ssc.awaitTermination()
  }
}
