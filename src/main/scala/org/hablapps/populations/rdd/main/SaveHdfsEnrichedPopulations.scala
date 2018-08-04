package org.hablapps
package populations
package main

import cats.MonadReader
import org.apache.spark.sql.SQLContext
import org.hablapps.etl.rdd.writer.instances._

case class SaveHdfsEnrichedPopulations[P[_]: MonadReader[?[_], SQLContext]] extends HdfsWriter[P, EnrichedPopulation]