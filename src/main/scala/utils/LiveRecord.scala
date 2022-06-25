package it.unibo.bd
package utils

import scala.util.{ Failure, Success, Try }

case class LiveRecord(
    protocol: String,
    sourceAddress: String,
    sourcePort: Long,
    destinationAddress: String,
    destinationPort: Long,
    packets: Long,
    bytes: Long,
    duration: Double,
    rate: Double,
)

object LiveRecord {

  def apply(r: Seq[String]): Option[LiveRecord] =
    (for {
      protocol <- if (r(2) == "tcp" || r(2) == "udp") Success(r(2)) else Failure(new IllegalStateException())
      sourceAddress = r(3)
      sourcePort <- Try(r(4).toLong)
      destinationAddress = r(6)
      destinationPort <- Try(r(7).toLong)
      packets <- Try(r(8).toLong)
      bytes <- Try(r(9).toLong)
      duration <- Try(r(14).toDouble)
      rate <- Try(r(30).toDouble)
    } yield LiveRecord(
      protocol,
      sourceAddress,
      sourcePort,
      destinationAddress,
      destinationPort,
      packets,
      bytes,
      duration,
      rate,
    )).toOption
}
