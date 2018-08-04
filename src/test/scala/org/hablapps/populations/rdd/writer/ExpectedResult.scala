package org.hablapps.populations.rdd.writer

import org.hablapps.populations.EnrichedPopulation

object ExpectedResult {

  def apply: Seq[EnrichedPopulation] = Seq(
    EnrichedPopulation("Zamora","ZA",1000),
    EnrichedPopulation("Madrid","MA",3000),
    EnrichedPopulation("Barcelona","BA",2000))

}
