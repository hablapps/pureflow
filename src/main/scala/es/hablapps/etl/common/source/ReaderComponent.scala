package es.hablapps.etl.common.source

import es.hablapps.etl.common.IAmASparkSQLQueristKey

trait ReaderComponent[K, T] {
  def reader: IAmASparkSQLQueristKey[K, T]
}
