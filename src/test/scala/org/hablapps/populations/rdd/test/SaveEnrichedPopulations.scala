package org.hablapps
package populations
package rdd
package test

import cats.MonadState
import org.hablapps.etl.rdd.writer.instances._

class SaveEnrichedPopulations[P[_]: MonadState[?[_], MapWriter.Env]]
extends MapWriter[P, EnrichedPopulation]

object SaveEnrichedPopulations {
  def apply[P[_]: MonadState[?[_], MapWriter.Env]] = new SaveEnrichedPopulations[P]
}
