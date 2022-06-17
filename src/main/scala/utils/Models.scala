package it.unibo.bd
package utils

import utils.NetworkProtocol.NetworkProtocol

import java.net.InetAddress
import java.time.format.{ DateTimeFormatter, DateTimeParseException }
import java.time.{ Instant, LocalDateTime, ZoneId }
import scala.util.Try

object NetworkProtocol extends Enumeration {
  type NetworkProtocol = Value

  val TCP: NetworkProtocol = Value
  val UDP: NetworkProtocol = Value
}

case class Record(
    stime: LocalDateTime,
    protocol: NetworkProtocol,
    saddr: String,
    sport: Long,
    dir: String,
    daddr: String,
    dport: Long,
    pkts: Long,
    bytes: Long,
    ltime: LocalDateTime,
    sbytes: Long,
    dbytes: Long,
    isDDoS: Boolean,
)

object Record {

  def apply(r: Seq[String]): Option[Record] =
    (for {
      stime <- Try(Instant.ofEpochMilli((r.head.toDouble * 1000).toLong).atZone(ZoneId.systemDefault()).toLocalDateTime)
      protocol <- Try(r(2)).map(c => if (c == "tcp") NetworkProtocol.TCP else NetworkProtocol.UDP)
      saddr = r(3)
      sport <- Try(r(4).toLong)
      dir = r(5)
      daddr = r(6)
      dport <- Try(r(7).toLong)
      pkts <- Try(r(8).toLong)
      bytes <- Try(r(9).toLong)
      ltime <- Try(Instant.ofEpochMilli((r(12).toDouble * 1000).toLong).atZone(ZoneId.systemDefault()).toLocalDateTime)
      sbytes <- Try(r(28).toLong)
      dbytes <- Try(r(29).toLong)
      isDDoS <- Try(r(34).toInt).map(p => if (p == 1) true else false)
    } yield Record(
      stime,
      protocol,
      saddr,
      sport,
      dir,
      daddr,
      dport,
      pkts,
      bytes,
      ltime,
      sbytes,
      dbytes,
      isDDoS,
    )).toOption
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
