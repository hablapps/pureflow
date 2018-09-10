package org.hablapps
package populations

trait Transforms[Collection[_], P[_]]{

  def EnrichPopulations(
    rawP: Collection[Population],
    cityAbbrev: Collection[City]): P[Collection[EnrichedPopulation]]
}
