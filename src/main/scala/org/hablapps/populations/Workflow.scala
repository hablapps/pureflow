package org.hablapps
package populations

import cats.Monad, cats.syntax.flatMap._, cats.syntax.functor._

import org.hablapps.etl._

case class Workflow[Col[_],P[_]: Monad](
  ReadCities: Reader[Col,P,City],
  ReadPopulations: Reader[Col,P,Population],
  // EnrichPopulations: Transform2[Col,P,Population,City,EnrichedPopulation],
  Transforms: Transforms[Col,P],
  SaveEnrichedPopulations: Writer[Col,P,EnrichedPopulation]){

  def run(cities_src: String,
      populations_src: String,
      enriched_dst: String): P[Unit] = for {
    cities <- ReadCities.valid(cities_src)
    populations <- ReadPopulations.valid(populations_src)
    // enrichedPopulations <- EnrichPopulations(populations, cities)
    enrichedPopulations <- Transforms.EnrichPopulations(populations, cities)
    _ <- SaveEnrichedPopulations.write(enrichedPopulations, enriched_dst)
  } yield ()
}
