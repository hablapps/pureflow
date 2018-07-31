package org.hablapps
package populations
package rdd
package main

import org.hablapps.etl.rdd.writer.instances._
import org.apache.hadoop.hbase.spark.HBaseContext
import cats.MonadReader

case class SaveEnrichedPopulations[P[_]: MonadReader[?[_], HBaseContext]]
extends PhoenixWriter[P, EnrichedPopulation]{
  val Columns = List("CITY", "P.STATE", "P.POPULATION")
}
