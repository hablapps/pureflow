package org.hablapps.etl
package populations
package workflow

trait Workflow[P[_]]{
  
  val ReadCities: Reader[P,City]
  val ReadPopulations: Reader[P,Population]
  val SaveEnrichedPopulations: Writer[P,EnrichedPopulation]

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