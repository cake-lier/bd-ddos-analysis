package it.unibo.bd

import org.apache.spark.{ SparkConf, SparkContext }
import utils.Utils._

import utils.Connection

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
