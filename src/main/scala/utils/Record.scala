package it.unibo.bd
package utils

import java.time.{ Instant, LocalDateTime, ZoneId }
import scala.util.{ Failure, Success, Try }

case class Record(
    startTime: LocalDateTime,
    protocol: String,
    sourceAddress: String,
    sourcePort: Long,
    direction: String,
    destinationAddress: String,
    destinationPort: Long,
    packets: Long,
    bytes: Long,
    endTime: LocalDateTime,
    duration: Double,
    rate: Double,
    isDDoS: Boolean,
)

object Record {

  def apply(r: Seq[String]): Option[Record] =
    (for {
      startTime <- Try(
        Instant.ofEpochMilli((r.head.toDouble * 1000).toLong).atZone(ZoneId.systemDefault()).toLocalDateTime,
      )
      protocol <- if (r(2) == "tcp" || r(2) == "udp") Success(r(2)) else Failure(new IllegalStateException())
      sourceAddress = r(3)
      sourcePort <- Try(r(4).toLong)
      direction = r(5)
      destinationAddress = r(6)
      destinationPort <- Try(r(7).toLong)
      packets <- Try(r(8).toLong)
      bytes <- Try(r(9).toLong)
      endTime <- Try(
        Instant.ofEpochMilli((r(12).toDouble * 1000).toLong).atZone(ZoneId.systemDefault()).toLocalDateTime,
      )
      duration <- Try(r(14).toDouble)
      rate <- Try(r(30).toDouble)
      isDDoS = r(34) == "1"
    } yield new Record(
      startTime,
      protocol,
      sourceAddress,
      sourcePort,
      direction,
      destinationAddress,
      destinationPort,
      packets,
      bytes,
      endTime,
      duration,
      rate,
      isDDoS,
    )).toOption
}
