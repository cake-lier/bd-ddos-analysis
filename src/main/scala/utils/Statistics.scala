package it.unibo.bd
package utils

case class Quartiles[T: Numeric](min: T, firstQuartile: T, secondQuartile: T, thirdQuartile: T, max: T)
case class Gaussian(mean: Double, stdDev: Double)
