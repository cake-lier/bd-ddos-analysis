package it.unibo.bd
package utils

import java.net.InetAddress
import java.time.LocalDateTime
import java.time.format.{ DateTimeFormatter, DateTimeParseException }
import scala.util.Try

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
