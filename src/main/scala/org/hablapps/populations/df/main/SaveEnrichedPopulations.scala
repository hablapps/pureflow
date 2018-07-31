package org.hablapps
package populations
package df
package main

import cats.MonadState
import org.hablapps.etl.df.writer.instances._

case class SaveEnrichedPopulations[P[_]: MonadState[?[_], MapWriter.Env]]
extends MapWriter[P, EnrichedPopulation]
