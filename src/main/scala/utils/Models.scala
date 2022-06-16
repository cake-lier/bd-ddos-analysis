package it.unibo.bd
package utils

import utils.NetworkProtocol.NetworkProtocol

import java.net.InetAddress
import java.time.LocalDateTime
import java.time.format.{ DateTimeFormatter, DateTimeParseException }
import scala.util.Try

object NetworkProtocol extends Enumeration {
  type NetworkProtocol = Value

  val TCP: NetworkProtocol = Value
  val UDP: NetworkProtocol = Value
}

case class Packet(
    index: Long,
    flowId: String,
    sourceIP: InetAddress,
    sourcePort: Long,
    destinationIP: InetAddress,
    destinationPort: Long,
    protocol: NetworkProtocol,
    timestamp: LocalDateTime,
    flowDuration: Long,
    byteSpeed: Double,
    packetSpeed: Double,
    isDDoS: Boolean,
)

object Packet {

  def apply(r: Seq[String]): Option[Packet] =
    (for {
      index <- Try(r.head.toLong)
      flowId = r(1)
      sourceIP <- Try(InetAddress.getByName(r(2)))
      sourcePort <- Try(r(3).toLong)
      destinationIP <- Try(InetAddress.getByName(r(4)))
      destinationPort <- Try(r(5).toLong)
      protocol <- Try(r(6).toInt).map(c => if (c == 6) NetworkProtocol.TCP else NetworkProtocol.UDP)
      timestamp <-
        Try {
          LocalDateTime.parse(r(7), DateTimeFormatter.ofPattern("dd/MM/yyyy hh:mm:ss a"))
        } recover { case _: DateTimeParseException =>
          LocalDateTime.parse(r(7), DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss"))
        }
      flowDuration <- Try(r(8).toLong)
      byteSpeed <- Try(r(21).toDouble)
      packetSpeed <- Try(r(22).toDouble)
      isDDoS = r(84) == "ddos"
    } yield Packet(
      index,
      flowId,
      sourceIP,
      sourcePort,
      destinationIP,
      destinationPort,
      protocol,
      timestamp,
      flowDuration,
      byteSpeed,
      packetSpeed,
      isDDoS,
    )).toOption
}

case class PortDescription(port: Long, protocol: NetworkProtocol, description: String)

object PortDescription {

  def apply(r: Seq[String]): Option[PortDescription] =
    (for {
      p <- Try(r.head.toLong)
      protocol = if (r(1) == "TCP") NetworkProtocol.TCP else NetworkProtocol.UDP
      description = r(2)
    } yield PortDescription(p, protocol, description)).toOption
}

case class GeoIP()

object GeoIP {
  def apply(r: Seq[String]): Option[GeoIP] = ???
}
