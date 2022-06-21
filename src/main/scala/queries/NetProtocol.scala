package it.unibo.bd
package queries

import org.apache.spark.{ SparkConf, SparkContext }

object NetProtocol {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("NetProtocol"))
    println(s"Application started at http://localhost:20888/proxy/${sc.applicationId}/\n")

    /*
    val packets =
      sc.textFile(s"${args(0)}/ddos-dataset.csv")
        .map(_.split(","))
        .map(Packet(_))
        .filter(_.isDefined)
        .map(_.get)

    val values =
      packets
        .map(c =>
          (
            if (c.protocol == NetworkProtocol.TCP) 1 else 0,
            if (c.protocol == NetworkProtocol.UDP) 1 else 0,
            if (c.protocol == NetworkProtocol.TCP && c.isDDoS) 1 else 0,
            if (c.protocol == NetworkProtocol.UDP && c.isDDoS) 1 else 0,
          ),
        )
        .reduce {
          case (
                (tcpCount1, udpCount1, tcpDDoSCount1, udpDDoSCount1),
                (tcpCount2, udpCount2, tcpDDoSCount2, udpDDoSCount2),
              ) =>
            (tcpCount1 + tcpCount2, udpCount1 + udpCount2, tcpDDoSCount1 + tcpDDoSCount2, udpDDoSCount1 + udpDDoSCount2)
        }

    println(s"TCP packets: ${values._1}")
    println(s"UDP packets: ${values._2}")
    println(s"TCP DDoS packets: ${values._3}")
    println(s"UDP DDoS packets: ${values._4}")
    println(s"Percentage of TCP DDoS packets over total: ${(values._2 / values._1.toDouble) * 100}")
    println(s"Percentage of UDP DDoS packets over total: ${(values._4 / values._3.toDouble) * 100}")

     */
  }
}
