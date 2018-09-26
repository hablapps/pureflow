package org.hablapps
package bbva


import cats.Monad
import cats.syntax.flatMap._
import cats.syntax.functor._
import org.hablapps.etl._

case class Workflow[Col[_], P[_]: Monad, BC](
  readRetRet: Reader[Col, P, RetRet],
  transforms: bbva.Transforms[Col, P, BC],
  SaveEnrichedPopulations: Writer[Col, P, Ret360Enriched]){

  def run(
           retret_src: String,
           enriched_dst: String,
           bcForeignExchange: BC
         ): P[Unit] =
    for {
      populations <- readRetRet.valid(retret_src)
      enrichedPopulations <- transforms.Enrich360(populations, bcForeignExchange)
      _ <- SaveEnrichedPopulations.write(enrichedPopulations, enriched_dst)
    } yield ()
}
