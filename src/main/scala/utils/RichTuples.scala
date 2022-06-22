package it.unibo.bd
package utils

import scala.math.Numeric.Implicits.infixNumericOps

object RichTuples {

  implicit class RichTuple2[A: Numeric, B: Numeric](self: (A, B)) {

    def plus(rhs: (A, B)): (A, B) = (self._1 + rhs._1, self._2 + rhs._2)

    def +(rhs: (A, B)): (A, B) = plus(rhs)
  }
}
