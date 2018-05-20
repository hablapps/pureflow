package es.hablapps.etl.common.sink

import es.hablapps.etl.common.IAmAHBaseSink

trait SinkComponent[T] {
  def sink: IAmAHBaseSink[T]
}
