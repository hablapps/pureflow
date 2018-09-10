package org.hablapps
package bbva


import cats.Monad
import cats.syntax.flatMap._
import cats.syntax.functor._
import org.hablapps.etl._

case class Workflow[Col[_], P[_]: Monad](
  readRetRet: Reader[Col, P, RetRet],
  transforms: Transforms[Col, P],
  SaveEnrichedPopulations: Writer[Col, P, Ret360Enriched]){

  def run(
           retret_src: String,
           enriched_dst: String,
           bcForeignExchange: transforms.SharedVariable[Map[String, String]]
         ): P[Unit] =
    for {
      populations <- readRetRet.valid(retret_src)
      enrichedPopulations <- transforms.Enrich360(populations, bcForeignExchange)
      _ <- SaveEnrichedPopulations.write(enrichedPopulations, enriched_dst)
    } yield ()
}
