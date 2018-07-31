package org.hablapps
package populations
package df
package main

import cats.data.State

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

import org.hablapps.etl._, df._
import naturally.mtl._

object Main{

  // Create workflow

  type Program[t] = State[(Map[String,Seq[_]], SQLContext),t]

  val workflow = Workflow[DataPhrame,Program](
    ReadCities[Program],
    ReadPopulations[Program],
    // EnrichPopulations[Program],
    Transforms[Program],
    SaveEnrichedPopulations[Program])

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
