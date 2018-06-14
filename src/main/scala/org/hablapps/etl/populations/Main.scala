package org.hablapps.etl
package v0

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

import cats.Monad, cats.data.{Reader => CReader}

object Main{

  // COMPILE TO READER

  type P[t] = CReader[(Map[String,Seq[_]], SparkContext, SQLContext),t]

  val program: P[Unit] = Workflow[P]("cities.src", "populations.parquet")(
    Monad[P],
    ??? /* ReadCities */,
    ??? /* ReadPopulations */,
    ???)
  
  // RUN READER

  val cfg = new SparkConf().setAppName("pipelines")
  val sc = SparkContext.getOrCreate(cfg)
  val sqlContext = new SQLContext(sc)
  val map = Map("cities" -> Seq(
    City("Madrid", "MA"),
    City("Barcelona", "BA"),
    City("Zamora", "ZA")))

  program((map, sc, sqlContext))
}