package org.hablapps
package populations
package rdd
package main

import org.hablapps.etl.rdd.writer.instances._
import org.apache.hadoop.hbase.spark.HBaseContext
import cats.MonadReader
import org.hablapps.bbva.Ret360Enriched

case class SaveEnrichedPopulations[P[_]: MonadReader[?[_], HBaseContext]]
extends PhoenixWriter[P, EnrichedPopulation]{
  val Columns = List("CITY", "P.STATE", "P.POPULATION")
}

case class SaveEnriched360[P[_]: MonadReader[?[_], HBaseContext]]
  extends PhoenixWriter[P, Ret360Enriched]{
  val Columns = List("CITY", "P.STATE", "P.POPULATION")
}
