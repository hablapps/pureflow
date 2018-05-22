package es.hablapps.etl.population.driver

import es.hablapps.etl.common.enricher.EnricherComponent
import es.hablapps.etl.common.pipeline.Pipeline
import es.hablapps.etl.common.sink.{PhoenixSink, SinkComponent}
import es.hablapps.etl.common.source.ReaderComponent
import es.hablapps.etl.common.{IAmAEnricherComponent, IAmAHBaseSink, IAmASparkSQLQueristKey}
import es.hablapps.etl.population.enricher.Enricher
import es.hablapps.etl.population.model.{EnrichedPopulation, PopulationPK, RawPopulation}
import es.hablapps.etl.population.sources.PopulationReader

object PopulationDriver {

  def main (args:Array [String]):Unit = {

    defaultPipeline().runPipeline(sc = null, hc = null)
  }

  def defaultPipeline() =
    new Pipeline[PopulationPK, RawPopulation, EnrichedPopulation]
    with ReaderComponent[PopulationPK, RawPopulation]
    with EnricherComponent[PopulationPK, RawPopulation, EnrichedPopulation]
    with SinkComponent[EnrichedPopulation]{
      override def pipeline: Pipeline = new Pipeline(partitionNumber = 1)
      override def reader: IAmASparkSQLQueristKey[PopulationPK, RawPopulation] = PopulationReader()
      override def enricher: IAmAEnricherComponent[PopulationPK, RawPopulation, EnrichedPopulation] = Enricher()

      override def sink: IAmAHBaseSink[EnrichedPopulation] = PhoenixSink()
    }
}
