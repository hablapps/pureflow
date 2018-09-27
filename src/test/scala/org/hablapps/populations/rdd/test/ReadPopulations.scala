package org.hablapps
package populations
package rdd
package test

import cats.MonadReader

import org.hablapps.etl.rdd.reader.instances.MapReader

class ReadPopulations[P[_]: MonadReader[?[_], MapReader.Env]]
extends MapReader[P, Population]

object ReadPopulations {
  def apply[P[_]: MonadReader[?[_], MapReader.Env]] = new ReadPopulations[P]
}
