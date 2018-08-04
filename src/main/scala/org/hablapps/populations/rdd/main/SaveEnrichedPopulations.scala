package org.hablapps
package populations
package rdd
package main

import cats.{MonadReader, MonadWriter}
import org.apache.hadoop.hbase.spark.HBaseContext
import org.hablapps.etl.rdd.writer.instances._

case class SaveEnrichedPopulations[P[_]: MonadReader[?[_], HBaseContext]]
  extends PhoenixWriter[P, EnrichedPopulation]{
  val Columns = List("CITY", "P.STATE", "P.POPULATION")
}
