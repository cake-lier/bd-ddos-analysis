package it.unibo.bd

import it.unibo.bd.utils.Record
import org.apache.log4j.{ Level, Logger }
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{ Seconds, StreamingContext }

import java.io.InputStream
import scala.io.Source

object App {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("DDoS").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    // Load S3 credentials
    val stream: InputStream = getClass.getResourceAsStream("/aws_credentials.txt")
    val lines = Source.fromInputStream(stream).getLines.toList

    // Create an RDD from the files in the given folder
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.fast.upload", "true")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.fast.upload.buffer", "bytebuffer")

    spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", lines.head)
    spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", lines(1))

    val ssc = StreamingContext.getOrCreate(null, functionToCreateContext _)
    ssc.start()
    ssc.awaitTermination()

    def updateFunction(
        newValues: Seq[(Int, Int)],
        oldValue: Option[(Int, Int, Int, Double)],
    ): Option[(Int, Int, Int, Double)] = {
      val oldValue2 = oldValue.getOrElse(0, 0, 0, 0.0)
      val totTweets = oldValue2._1 + newValues.map(_._1).sum
      val totSentiment = oldValue2._2 + newValues.map(_._2).sum
      val countSentiment = oldValue2._3 + newValues.size
      val avgSentiment = totSentiment.toDouble / countSentiment.toDouble
      Some((totTweets, totSentiment, countSentiment, avgSentiment))
    }

    def functionToCreateContext(): StreamingContext = {
      val newSsc = new StreamingContext(spark.sparkContext, Seconds(1))
      val lines = newSsc.socketTextStream(null, null, StorageLevel.MEMORY_AND_DISK_SER)
      val records =
        lines
          .map(_.replace("\"", ""))
          .map(_.split(";"))
          .map(Record(_))
          .filter(_.isDefined)
          .map(_.get)

      val flows =
        records
          .map(r =>
            (
              (r.sourceAddress, r.destinationAddress, r.sourcePort, r.destinationPort, r.protocol),
              (r.packets, r.bytes, r.rate, if (r.duration > 0.0) r.bytes / r.duration else 0.0),
            ),
          )
      /*
      val cumulativeCityCounts = cities.updateStateByKey(updateFunction)
      cumulativeCityCounts
        .mapValues(v => (v._1, v._2, v._3, Math.round(v._4 * 100).toDouble / 100))
        .map({ case (k, v) => (v, k) })
        .transform({ rdd => rdd.sortByKey(ascending = false) })
        .print()*/
      newSsc.checkpoint(null)
      newSsc
    }
  }
}
