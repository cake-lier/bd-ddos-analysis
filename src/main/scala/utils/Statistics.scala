package it.unibo.bd
package utils

case class Quartiles[T: Numeric](min: T, firstQuartile: T, secondQuartile: T, thirdQuartile: T, max: T)

case class Gaussian(mean: Double, stdDev: Double)

object Functions {

  def gaussian(avg: Double, stdDev: Double)(x: Double): Double =
    1 / (stdDev * math.sqrt(2 * math.Pi)) * math.exp(-0.5 * math.pow(x - avg, 2) / math.pow(stdDev, 2))
}
