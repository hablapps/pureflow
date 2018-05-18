package es.hablapps.etl.common

import es.hablapps.etl.common.IAmARawGeneric.Source.Source

trait IAmARawGeneric[T] {
  def srcTable: String
  def source: Source
  def rawPK: T
}

object IAmARawGeneric {
  object Source extends Enumeration {
    type Source = Value
    val DATA_EXAMPLE_1, DATA_EXAMPLE_2 = Value
  }
}