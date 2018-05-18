package es.hablapps.etl.population.enricher

import es.hablapps.etl.common.IAmAEnricherComponent
import es.hablapps.etl.population.model.{EnrichedPopulation, PopulationPK, RawPopulation}
import org.apache.spark.rdd.RDD

case class Enricher() extends IAmAEnricherComponent[PopulationPK, RawPopulation, EnrichedPopulation]{
  override def enrich(raw: RDD[(PopulationPK, RawPopulation)]): RDD[EnrichedPopulation] = raw.mapPartitions(p => {
    val enricher = PopulationEnricherFactory.init()
    p.map(r => enricher.enrich(r._2))
  })
}
