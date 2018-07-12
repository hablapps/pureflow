package org.hablapps
package populations
package df
package main


import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

import org.hablapps.etl._, df._

object Main{

  // Create workflow

  val workflow = Workflow[DataPhrame,Program](
    ReadCities,
    ReadPopulations,
    // EnrichPopulations[Program],
    Transforms[Program],
    SaveEnrichedPopulations)

  // Compile workflow

  val compiledProgram =
    workflow.run("cities.seq", "populations.parquet", "enriched.hb")

  // Run workflow

  val cfg = new SparkConf().setAppName("pipelines")
  val sc = SparkContext.getOrCreate(cfg)
  val sqlContext = new SQLContext(sc)
  val map = Map("cities" -> Seq(
    City("Madrid", "MA"),
    City("Barcelona", "BA"),
    City("Zamora", "ZA")))

  compiledProgram.run((map, sqlContext))
}
