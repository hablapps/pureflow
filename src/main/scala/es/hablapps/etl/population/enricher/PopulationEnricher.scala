package es.hablapps.etl.population.enricher

import es.hablapps.etl.common.enricher.IAmAEnricherTransaction
import es.hablapps.etl.population.cache.ShortName
import es.hablapps.etl.population.model.{EnrichedPopulation, IAmARawPopulation}
import org.slf4j.{Logger, LoggerFactory}

trait PopulationEnricher extends IAmAEnricherTransaction[IAmARawPopulation, EnrichedPopulation] {

  def rawPopulationEnricher: PopulationEnricher
  def shortNameCache: ShortName

  override def enrich: IAmARawPopulation => EnrichedPopulation = (rawTx: IAmARawPopulation) => rawPopulationEnricher(rawTx)

  class PopulationEnricher(){
    lazy val LOG: Logger = LoggerFactory.getLogger(classOf[PopulationEnricher])

    def apply(raw: IAmARawPopulation): EnrichedPopulation = {

      EnrichedPopulation(
        raw.rawName,
        shortNameCache.get(raw.rawName),
        raw.rawPopulation.toLong
      )
    }
  }

}

