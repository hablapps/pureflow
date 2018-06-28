package org.hablapps
package populations
package workflow

import org.apache.spark.rdd.RDD

object EnrichPopulations{

  def apply(rawP: RDD[Population],
    cityAbbrev: RDD[City]): RDD[EnrichedPopulation] =
    rawP.map(p => (p.name, p))
      .join(cityAbbrev.map( c => (c.name, c)))
      .map{ case (k, (Population(_,p), City(_,a))) =>
        EnrichedPopulation(k, a, p.toLong)
      }
}