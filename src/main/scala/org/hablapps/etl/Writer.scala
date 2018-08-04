package org.hablapps.etl

abstract class Writer[Collection[_], P[_], T]{
  type K
  def write(dataset: Collection[T], destination: K): P[Unit]
}

object Writer {
  type Aux[Collection[_], P[_], T, K0] = Writer[Collection, P, T] { type K = K0 }
}