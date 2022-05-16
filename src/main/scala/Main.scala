package it.unibo.bd

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd.RDD

object Main extends App {
  val sc = new SparkContext(new SparkConf().setAppName("Main"))
  println(s"Application started at http://localhost:20888/proxy/${sc.applicationId}/\n")

  val unbalancedDatasetURL = s"s3://${args(0)}/unbalaced_20_80_dataset.csv"
  val unbalancedDataset = sc.textFile(unbalancedDatasetURL)

  println(unbalancedDataset.first())
  println()

  val balancedDatasetURL = s"s3://${args(0)}/final_dataset.csv"
  val balancedDataset = sc.textFile(balancedDatasetURL)

  implicit class RichRDD[A](self: RDD[A]) {
    def skip(n: Int): RDD[A] = self.zipWithIndex().filter(_._2 >= n).map(_._1)
  }

  val dataset = unbalancedDataset.skip(1) ++ balancedDataset.skip(1)

  /* TO DO:
   * 0. Analisi iniziali banali (numero di righe per dataset/totali, ...)
   * 1. Verificare quali campi sono inutili e quali no:
   *    1. Verificare quali hanno tutti lo stesso valore
   *    2. Verificare quali sono effettivamente incomprensibili per la nostra analisi
   * 2. Spezzettare il dataset in colonne e tenere solo quelle in corrispondenza agli indici di interesse
   * 3. Costruire la case class con il formato dei dati restanti
   * 4. $$$ PROFIT $$$
   */
}
