package it.unibo.bd
package utils

import scala.util.Try

case class PortDescription(
    port: Long,
    protocol: String,
    description: String,
)

object PortDescription {

  def apply(r: Seq[String]): Option[PortDescription] =
    (for {
      port <- Try(r.head.toLong)
      protocol <- Try(r(1).toLowerCase)
      description = r(2)
    } yield new PortDescription(port, protocol, description)).toOption
}
