package org.hablapps
package populations
package rdd
package test

import cats.MonadState
import org.hablapps.etl.rdd.writer.instances._

case class SaveEnrichedPopulations[P[_]: MonadState[?[_], MapWriter.Env]]
extends MapWriter[P, EnrichedPopulation]
