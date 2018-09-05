package org.hablapps
package populations

trait Transforms[Collection[_], P[_]]{

  val EnrichedPopulation: Transform2[...]

  def EnrichPopulations(
    rawP: Collection[Population],
    cityAbbrev: Collection[City]): P[Collection[EnrichedPopulation]]
}
