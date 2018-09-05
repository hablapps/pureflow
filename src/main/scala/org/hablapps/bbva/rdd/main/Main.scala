package org.hablapps
package bbva
package rdd
package main

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.spark.sql.SQLContext
import cats.data.{Reader => CReader}
import org.hablapps.populations.rdd.main.SaveEnrichedPopulations
import org.hablapps.etl._


object MainBbva{

  // Create workflow

  type Program[t] = CReader[(SparkContext, SQLContext, HBaseContext), t]

  val workflow = Workflow[RDD, Program](
    ReadRetRet[Program],
    // EnrichPopulations[Program],
    Transforms[Program],
    SaveEnrichedPopulations[Program])


  // Run workflow

  val cfg = new SparkConf().setAppName("pipelines")
  val sc = SparkContext.getOrCreate(cfg)
  val sqlContext = new SQLContext(sc)
  val hc: HBaseContext = ???
  val map = Map("USX" -> "USD")
  // Compile workflow

  val compiledProgram: Program[Unit] =
    workflow.run("populations.parquet", "enriched.hb", sc.broadcast(map))

  compiledProgram((sc, sqlContext, hc))
}
