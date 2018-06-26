package org.hablapps.etl
package populations
package main

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.spark.sql.SQLContext

import workflow._

object Main{

  // Create workflow

  val workflow = Workflow[Program](
    main.ReadCities,
    main.ReadPopulations,
    main.SaveEnrichedPopulations)

  // Compile workflow

  val compiledProgram = workflow.run("cities.seq", "populations.parquet", "enriched.hb")

  // Run workflow

  val cfg = new SparkConf().setAppName("pipelines")
  val sc = SparkContext.getOrCreate(cfg)
  val sqlContext = new SQLContext(sc)
  val hc: HBaseContext = ???
  val map = Map("cities" -> Seq(
    City("Madrid", "MA"),
    City("Barcelona", "BA"),
    City("Zamora", "ZA")))

  compiledProgram((map, sc, sqlContext, hc))
}