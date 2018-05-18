package es.hablapps.etl.population.enricher
import es.hablapps.etl.population.cache.ShortName

object PopulationEnricherFactory {

  def init(): PopulationEnricher = new PopulationEnricher {
    override lazy val rawPopulationEnricher: PopulationEnricher = new PopulationEnricher()
    override def shortNameCache: ShortName = new ShortName
  }
}
