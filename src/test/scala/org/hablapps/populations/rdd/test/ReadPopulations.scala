package org.hablapps
package populations
package rdd
package test

import cats.MonadReader

import org.hablapps.etl.rdd.reader.instances.MapReader

case class ReadPopulations[P[_]: MonadReader[?[_], MapReader.Env]]
extends MapReader[P, Population]
