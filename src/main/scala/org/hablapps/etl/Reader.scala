package org.hablapps.etl

import cats.data.Validated

abstract class Reader[Collection[_], P[_], T] extends java.io.Serializable{
  type Data
  type Error

  def load(from: String): P[Collection[Data]]

  def apply(from: String): P[Collection[Validated[(Data,List[Error]), T]]]

  def valid(from: String): P[Collection[T]]

  def invalid(from: String): P[Collection[(Data, List[Error])]]
}
