package org.hablapps.etl
package v0

import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.spark.SparkContext

import cats.Monad, cats.syntax.flatMap._, cats.syntax.applicative._, cats.syntax.functor._

object Workflow{

  def apply[P[_]](cities_src: String, 
      populations_src: String)(implicit 
      M: Monad[P],
      ReadCities: Reader[P,City],
      ReadPopulations: Reader[P,Population],
      hc: HBaseContext): P[Unit] = for {
    cities <- ReadCities.valid(cities_src)
    populations <- ReadPopulations.valid(populations_src)
    enrichedPopulations <- EnrichPopulations(populations, cities).pure
    _ <- SaveEnrichedPopulations(hc, enrichedPopulations).pure
  } yield ()

}