package it.unibo.bd
package utils

object Metric {

  def metric(
      destinationPort: Long,
      destinationAddress: String,
      packets: Long,
      bytes: Long,
      rate: Double,
      byteRate: Double,
      flowRate: Double,
      flowByteRate: Double,
  ): Double =
    ((if (destinationPort == 80L) 1 else 0) +
      (if (Set("192.168.100.3", "192.168.100.6", "192.168.100.7", "192.168.100.5")(destinationAddress)) 1 else 0) +
      (1 - math.min(math.abs(packets - 5.909105626202674) / (3 * 3.2784993361685184), 1.0)) +
      (1 - math.min(math.abs(bytes - 529.8851801659653) / (3 * 240.0539442965255), 1.0)) +
      (1 - math.min(math.abs(rate - 0.27486262284753876) / (3 * 0.18712897621757338), 1.0)) +
      (1 - math.min(math.abs(byteRate - 33.86546680087338) / (3 * 17.010164888097375), 1.0)) +
      (1 - math.min(math.abs(flowRate - 0.27403419840566284) / (3 * 0.15597820418003489), 1.0)) +
      (1 - math.min(math.abs(flowByteRate - 26.483191236399332) / (3 * 14.608112880857705), 1.0))) / 8

  def checkDDoS(
      destinationPort: Long,
      destinationAddress: String,
      packets: Long,
      bytes: Long,
      rate: Double,
      byteRate: Double,
      flowRate: Double,
      flowByteRate: Double,
  ): Boolean =
    metric(destinationPort, destinationAddress, packets, bytes, rate, byteRate, flowRate, flowByteRate) > 0.5
}
