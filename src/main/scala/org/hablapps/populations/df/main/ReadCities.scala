package org.hablapps
package populations
package df
package main

import org.hablapps.etl.df.reader.instances._
import cats.MonadReader

case class ReadCities[P[_]: MonadReader[?[_], MapReader.Env]]
extends MapReader[P, City]
