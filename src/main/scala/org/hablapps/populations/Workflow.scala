package org.hablapps
package populations

import cats.Monad, cats.syntax.flatMap._, cats.syntax.functor._

import org.hablapps.etl._

case class Workflow[Col[_],P[_]: Monad, S, K](
  ReadCities: Reader.Aux[Col,P,City,S],
  ReadPopulations: Reader.Aux[Col,P,Population,S],
  // EnrichPopulations: Transform2[Col,P,Population,City,EnrichedPopulation],
  Transforms: Transforms[Col,P],
  SaveEnrichedPopulations: Writer.Aux[Col,P,EnrichedPopulation,K]){

  def run(cities_src: S,
      populations_src: S,
      enriched_dst: K): P[Unit] = for {
    cities <- ReadCities.valid(cities_src)
    populations <- ReadPopulations.valid(populations_src)
    // enrichedPopulations <- EnrichPopulations(populations, cities)
    enrichedPopulations <- Transforms.EnrichPopulations(populations, cities)
    _ <- SaveEnrichedPopulations.write(enrichedPopulations, enriched_dst)
  } yield ()
}
