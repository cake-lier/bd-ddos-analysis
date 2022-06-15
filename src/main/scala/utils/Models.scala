package it.unibo.bd
package utils

import java.net.InetAddress
import java.time.LocalDateTime
import java.time.format.{ DateTimeFormatter, DateTimeParseException }
import scala.util.Try

sealed trait NetworkProtocol
case object TCP extends NetworkProtocol
case object UDP extends NetworkProtocol

case class Connection(
    index: Long,
    flowId: String,
    sourceIP: InetAddress,
    sourcePort: Long,
    destinationIP: InetAddress,
    destinationPort: Long,
    protocol: NetworkProtocol,
    timestamp: LocalDateTime,
    flowDuration: Long,
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
      protocol <- Try(r(6).toInt).map(c => if (c == 6) TCP else UDP)
      timestamp <-
        Try {
          LocalDateTime.parse(r(7), DateTimeFormatter.ofPattern("dd/MM/yyyy hh:mm:ss a"))
        } recover { case _: DateTimeParseException =>
          LocalDateTime.parse(r(7), DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss"))
        }
      flowDuration <- Try(r(8).toLong)
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
      flowDuration,
      isDDoS,
    )).toOption
}

case class PortDescription(port: Long, protocol: NetworkProtocol, description: String)

object PortDescription {

  def apply(r: Seq[String]): Option[PortDescription] = (for {
    p <- Try(r.head.toLong)
    prot = if (r(1) == "TCP") TCP else UDP
    desc = r(2)
  } yield new PortDescription(p, prot, desc)).toOption
}
