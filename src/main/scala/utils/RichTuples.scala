package it.unibo.bd
package utils

import scala.math.Numeric.Implicits.infixNumericOps

object RichTuples {

  implicit class RichTuple2[A: Numeric, B: Numeric](self: (A, B)) {

    def plus(rhs: (A, B)): (A, B) = (self._1 + rhs._1, self._2 + rhs._2)

    def +(rhs: (A, B)): (A, B) = plus(rhs)
  }

  implicit class RichTuple4[A: Numeric, B: Numeric, C: Numeric, D: Numeric](self: (A, B, C, D)) {

    def plus(rhs: (A, B, C, D)): (A, B, C, D) =
      (self._1 + rhs._1, self._2 + rhs._2, self._3 + rhs._3, self._4 + rhs._4)

    def +(rhs: (A, B, C, D)): (A, B, C, D) = plus(rhs)
  }

  implicit class RichTuple6[A: Numeric, B: Numeric, C: Numeric, D: Numeric, E: Numeric, F: Numeric](
      self: (A, B, C, D, E, F),
  ) {

    def plus(rhs: (A, B, C, D, E, F)): (A, B, C, D, E, F) = (
      self._1 + rhs._1,
      self._2 + rhs._2,
      self._3 + rhs._3,
      self._4 + rhs._4,
      self._5 + rhs._5,
      self._6 + rhs._6,
    )

    def +(rhs: (A, B, C, D, E, F)): (A, B, C, D, E, F) = plus(rhs)
  }
}
