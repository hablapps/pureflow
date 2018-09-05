package org.hablapps
package bbva


import cats.Monad
import cats.syntax.flatMap._
import cats.syntax.functor._
import org.apache.spark.broadcast.Broadcast
import org.hablapps.etl._

case class Workflow[Col[_], P[_]: Monad](
  ReadRetRet: Reader[Col, P, RetRet],
  // EnrichPopulations: Transform2[Col,P,Population,City,EnrichedPopulation],
  Transforms: Transforms[Col, P],
  SaveEnrichedPopulations: Writer[Col, P, Ret360Enriched]){

  def run(
           retret_src: String,
           enriched_dst: String,
           bcForeignExchange: Broadcast[Map[String, String]]
         ): P[Unit] =
    for {
      populations <- ReadRetRet.valid(retret_src)
      // enrichedPopulations <- EnrichPopulations(populations, cities)
      enrichedPopulations <- Transforms.Enrich360(populations, bcForeignExchange)
      _ <- SaveEnrichedPopulations.write(enrichedPopulations, enriched_dst)
    } yield ()
}
