package it.unibo.bd
package utils

object RichTuples {

  implicit class RichTuple2[A: Numeric, B: Numeric](self: (A, B)) {

    def plus(rhs: (A, B)): (A, B) =
      (implicitly[Numeric[A]].plus(self._1, rhs._1), implicitly[Numeric[B]].plus(self._2, rhs._2))

    def +(rhs: (A, B)): (A, B) = plus(rhs)
  }
}
