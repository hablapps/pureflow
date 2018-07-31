package org.hablapps.etl

abstract class Writer[Collection[_], P[_], T]{
  def write(dataset: Collection[T], destination: String): P[Unit]
}
