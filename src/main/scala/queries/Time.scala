package it.unibo.bd
package queries

import utils.Record

import org.apache.spark.{ SparkConf, SparkContext }

import java.time.LocalDateTime

object Time {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("Time"))
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

    val minMaxDate =
      recordDataset
        .filter(_.isDDoS)
        .map(r => (r.startTime, r.startTime))
        .reduce { case ((accMax, accMin), (r1, r2)) => (maxDate(accMax, r1), minDate(accMin, r2)) }

    println(s"Min timestamp: ${minMaxDate._2}")
    println(s"Max timestamp: ${minMaxDate._1}")
  }

  def minDate(date1: LocalDateTime, date2: LocalDateTime): LocalDateTime = {
    if (date1.isBefore(date2)) date1 else date2
  }

  def maxDate(date1: LocalDateTime, date2: LocalDateTime): LocalDateTime = {
    if (date1.isAfter(date2)) date1 else date2
  }
}
