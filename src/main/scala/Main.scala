package it.unibo.bd

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd.RDD

import java.net.InetAddress
import java.time.LocalDateTime
import java.time.format.{ DateTimeFormatter, DateTimeParseException }
import scala.reflect.ClassTag
import scala.util.Try

object Main extends App {
  val sc = new SparkContext(new SparkConf().setAppName("Main"))
  println(s"Application started at http://localhost:20888/proxy/${sc.applicationId}/\n")

  val dataset =
    if (args(0).toBoolean) {
      val unbalancedDatasetURL = s"s3://${args(1)}/unbalaced_20_80_dataset.csv"
      val unbalancedDataset = sc.textFile(unbalancedDatasetURL)

      println(unbalancedDataset.first())
      println()

      val balancedDatasetURL = s"s3://${args(1)}/final_dataset.csv"
      val balancedDataset = sc.textFile(balancedDatasetURL)

      unbalancedDataset ++ balancedDataset.skip(1)
    } else {
      sc.textFile("dataset.csv")
    }

  implicit class RichRDD[A: ClassTag](self: RDD[A]) {

    def skip(n: Int): RDD[A] = self.zipWithIndex().filter(_._2 >= n).map(_._1)
  }

  implicit class RichSeqRDD[A: ClassTag](self: RDD[Seq[A]]) {

    def toColumns(h: Seq[String]): RDD[(String, Seq[A])] =
      self
        .flatMap(_.zipWithIndex)
        .groupBy(_._2)
        .sortByKey()
        .map { case (i, vs) => (h(i), vs.map(_._1).toSeq) }
  }

  val totalRows = dataset.count() - 1

  println(totalRows)

  val table = dataset.skip(1).map(_.split(",").toSeq)
  val header = "Index" +: dataset.first().split(",").tail.toSeq

  println(header.zipWithIndex.map(_.swap))

  println(
    table
      .toColumns(header)
      .map { case (h, vs) => (h, vs.count(_ == "")) }
      .filter(_._2 == 1)
      .collect()
      .mkString("[", ", ", "]"),
  )

  println(
    table
      .toColumns(header)
      .map { case (h, vs) => (h, vs.distinct.size) }
      .filter(_._2 == 1)
      .collect()
      .mkString("[", ", ", "]"),
  )

  case class Connection(
      index: Long,
      flowId: String,
      sourceIP: InetAddress,
      sourcePort: Long,
      destinationIP: InetAddress,
      destinationPort: Long,
      protocol: Int,
      timestamp: LocalDateTime,
      isDDoS: Boolean,
  )

  object Connection {

    def apply(r: Seq[String]): Option[Connection] =
      (for {
        index <- Try(r.head.toLong)
        flowId = r(1)
        sourceIP <- Try(InetAddress.getByName(r(2)))
        sourcePort <- Try(r(3).toLong)
        destinationIP <- Try(InetAddress.getByName(r(4)))
        destinationPort <- Try(r(5).toLong)
        protocol <- Try(r(6).toInt)
        timestamp <-
          Try {
            LocalDateTime.parse(r(7), DateTimeFormatter.ofPattern("dd/MM/yyyy hh:mm:ss a"))
          } recover { case _: DateTimeParseException =>
            LocalDateTime.parse(r(7), DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss"))
          }
        isDDoS = r(84) == "ddos"
      } yield Connection(
        index,
        flowId,
        sourceIP,
        sourcePort,
        destinationIP,
        destinationPort,
        protocol,
        timestamp,
        isDDoS,
      )).toOption
  }

  val parsedTable = table.map(Connection(_)).filter(_.isDefined).map(_.get)

  println(
    parsedTable.count(),
    parsedTable.first(),
  )

  val totalDDoSRows = parsedTable.filter(_.isDDoS).count()

  println(
    totalDDoSRows,
    (totalDDoSRows / totalRows.toDouble) * 100,
  )

  val totalFlows = parsedTable.groupBy(_.flowId).count()

  val totalPureFlows =
    parsedTable
      .groupBy(_.flowId)
      .map(g => if (g._2.forall(_.isDDoS) || g._2.forall(!_.isDDoS)) Some(g._2.head.isDDoS) else None)
      .filter(_.isDefined)
      .map(_.get)
      .count()

  val totalDDoSFlows =
    parsedTable
      .groupBy(_.flowId)
      .filter(g => g._2.forall(_.isDDoS))
      .count()

  println(
    totalFlows,
    totalPureFlows,
    totalDDoSFlows,
    (totalDDoSFlows / totalFlows.toDouble) * 100,
  )

  /* TO DO:
   * 0. Analisi iniziali banali (numero di righe per dataset (?), ...)
   * 1. Verificare quali campi sono inutili e quali no:
   *    1. Verificare quali sono effettivamente incomprensibili per la nostra analisi
   *    2. Tenere solo quelle in corrispondenza agli indici di interesse
   * 3. Costruire la case class con il formato dei dati giusti
   * 4. $$$ PROFIT $$$
   */
}
