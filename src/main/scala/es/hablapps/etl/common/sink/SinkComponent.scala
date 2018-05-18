package es.hablapps.etl.common.sink

import es.hablapps.etl.common.IAmAHBaseSink

trait SinkComponent[T] {
  def phoenixTransactionSink : IAmAHBaseSink[T]
}
