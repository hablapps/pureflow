package org.hablapps
package translation

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import cats.data.{Reader => CReader}
import workflows.{Translate, ReadConfigWorkflow}
import classes._
import lib._

object Main {

  type Env = SQLContext
  type Program[A] = CReader[Env, A]

  // Readers & Writers

  val parquetReader = new ParquetReader[Env](identity)
  val parquetWriter = new ParquetWriter[Env](identity)

  // Create read config workflow

  val readConfWF = ReadConfigWorkflow[Program](
    parquetReader,
    parquetReader,
    parquetReader)

  // Create translate workflow

  val workflow = Translate[Program](
    parquetReader,
    parquetReader,
    parquetWriter,
    parquetWriter)

  // Compile workflow

  val compiledProgram =
    for {
      config <- readConfWF.run(
        "processId",
        "criteriaSrc",
        "crossSrc",
        "fieldSrc")
      translated <- workflow.run(
        "inputSrc",
        config,
        "outputSrc",
        "discardedSrc")
    } yield translated

  // Run workflow

  val cfg = new SparkConf().setAppName("translation")
  val sc = SparkContext.getOrCreate(cfg)
  val sqlContext = new SQLContext(sc)

  compiledProgram(sqlContext)

}
