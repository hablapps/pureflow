package org.hablapps
package translation
package workflow

import org.apache.spark.{SparkConf, SparkContext}
// import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.spark.sql.SQLContext
//
// import workflow._
// import org.hablapps.etl._

import cats.data.{Reader => CReader}

object Main {

  type Env = SQLContext
  type Program[A] = CReader[Env, A]

  // Create workflow

  val parquetReader = new ParquetReader[Env](identity)
  val parquetWriter = new ParquetWriter[Env](identity)
  val workflow = WorkflowDF[Program](
    parquetReader,
    parquetReader,
    parquetWriter)

  // Compile workflow

  val compiledProgram = workflow.run(
    "inputSrc",
    List.empty[TranslateColumnConf],
    "outputSrc")

  // Run workflow

  val cfg = new SparkConf().setAppName("translation")
  val sc = SparkContext.getOrCreate(cfg)
  val sqlContext = new SQLContext(sc)

  compiledProgram(sqlContext)

//  val hc: HBaseContext = ???
//  val map = Map("cities" -> Seq(
//    City("Madrid", "MA"),
//    City("Barcelona", "BA"),
//    City("Zamora", "ZA")))
//
//  compiledProgram((map, sc, sqlContext, hc))
}
