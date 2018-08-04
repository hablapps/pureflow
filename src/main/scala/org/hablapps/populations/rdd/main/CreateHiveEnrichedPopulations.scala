package org.hablapps.populations.rdd.main

import cats.MonadReader
import org.apache.spark.sql.SQLContext
import org.hablapps.etl.rdd.HiveWriter
import org.hablapps.populations.EnrichedPopulation


case class CreateHiveEnrichedPopulations[P[_]: MonadReader[?[_], SQLContext]] extends HiveWriter[P, EnrichedPopulation]