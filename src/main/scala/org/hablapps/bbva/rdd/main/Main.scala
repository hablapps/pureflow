package org.hablapps
package bbva
package rdd
package main

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.spark.sql.SQLContext
import cats.data.{Reader => CReader}
import org.apache.spark.broadcast.Broadcast
import org.hablapps.populations.rdd.main.SaveEnriched360
import org.hablapps.etl._


object MainBbva{

  // Create workflow

  type Program[t] = CReader[(SparkContext, SQLContext, HBaseContext), t]

  private val transforms: Transforms[Program] = Transforms[Program]
  private val readRetRet: ReadRetRet[Program] = ReadRetRet[Program]
  private val SaveEnrichedPopulations: SaveEnriched360[Program] = SaveEnriched360[Program]

  val workflow = Workflow[RDD, Program](
    readRetRet,
    // EnrichPopulations[Program],
    transforms,
    SaveEnrichedPopulations)


  // Run workflow

  val cfg = new SparkConf().setAppName("pipelines")
  val sc = SparkContext.getOrCreate(cfg)
  val sqlContext = new SQLContext(sc)
  val hc: HBaseContext = ???
  val map = Map("USX" -> "USD")
  // Compile workflow

  private val value: Broadcast[Map[String, String]] = sc.broadcast(map)
  val compiledProgram: Program[Unit] =
    workflow.run("populations.parquet", "enriched.hb", value)

  compiledProgram((sc, sqlContext, hc))
}
