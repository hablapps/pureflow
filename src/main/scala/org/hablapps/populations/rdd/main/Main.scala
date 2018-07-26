package org.hablapps
package populations
package rdd
package main

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.spark.sql.SQLContext

import cats.data.{Reader => CReader}

import org.hablapps.etl._

object Main{

  // Create workflow

  type Program[t] =
    CReader[(Map[String,Seq[_]], SparkContext, SQLContext, HBaseContext),t]

  val workflow = Workflow[RDD,Program](
    ReadCities.lift[Program],
    ReadPopulations.lift[Program],
    Transforms[Program],
    SaveEnrichedPopulations.lift[Program])

  // Compiled workflow

  val compiledProgram: Program[Unit] =
    workflow.run("cities.seq", "populations.parquet", "enriched.hb")

  // Run compiled workflow

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
