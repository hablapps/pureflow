package org.hablapps
package populations
package workflow

import org.hablapps.etl.rdd._

case class Workflow[P[_]](
  ReadCities: RDDReader[P,City],
  ReadPopulations: RDDReader[P,Population],
  SaveEnrichedPopulations: Writer[P,EnrichedPopulation]){

  import cats.Monad, cats.syntax.flatMap._, cats.syntax.applicative._, cats.syntax.functor._

  def run(cities_src: String,
      populations_src: String,
      enriched_dst: String)(implicit
      M: Monad[P]): P[Unit] = for {
    cities <- ReadCities.valid(cities_src)
    populations <- ReadPopulations.valid(populations_src)
    enrichedPopulations <- EnrichPopulations(populations, cities).pure
    _ <- SaveEnrichedPopulations.write(enrichedPopulations, enriched_dst)
  } yield ()
}