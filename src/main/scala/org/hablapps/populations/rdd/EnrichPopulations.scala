package org.hablapps
package populations
package rdd

import cats.data.Kleisli
import cats.Applicative, cats.syntax.applicative._

import org.apache.spark.rdd.RDD

import org.hablapps.etl._

class Transforms[P[_]: Applicative] extends populations.Transforms[RDD, P]{

  def EnrichPopulations(rawP: RDD[Population], cityAbbrev: RDD[City]) =
    rawP.map(p => (p.name, p))
      .join(cityAbbrev.map( c => (c.name, c)))
      .map{ case (k, (Population(_,p), City(_,a))) =>
        EnrichedPopulation(k, a, p)
      }.pure[P]
}

object Transforms {
  def apply[P[_]: Applicative] = new Transforms[P]
}

object EnrichPopulations{

  def apply[P[_]: Applicative]: Transform2[RDD,P,Population,City,EnrichedPopulation] =
    Kleisli{
      case (rawP, cityAbbrev) =>
        Transforms[P].EnrichPopulations(rawP, cityAbbrev)
    }
}
