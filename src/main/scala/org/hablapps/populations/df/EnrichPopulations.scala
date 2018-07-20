package org.hablapps
package populations
package df

import cats.Applicative, cats.syntax.applicative._

import org.hablapps.etl.df._

case class Transforms[P[_]: Applicative]
extends populations.Transforms[DataPhrame,P]{

  def EnrichPopulations(
    rawP: DataPhrame[Population],
    cityAbbrev: DataPhrame[City]): P[DataPhrame[EnrichedPopulation]] =
    rawP.join(cityAbbrev, List("name"))
      .withColumnRenamed("abbrev", "shortName")
      .pure[P]
}
