package org.hablapps.etl

import cats.data.Validated

abstract class Reader[Collection[_], P[_], T] extends java.io.Serializable{
  type Data
  type Error
  type S

  def load(from: S): P[Collection[Data]]

  def apply(from: S): P[Collection[Validated[(Data,List[Error]), T]]]

  def valid(from: S): P[Collection[T]]

  def invalid(from: S): P[Collection[(Data, List[Error])]]
}

object Reader {
  type Aux[Collection[_], P[_], T, S0] = Reader[Collection, P, T] { type S = S0 }
}