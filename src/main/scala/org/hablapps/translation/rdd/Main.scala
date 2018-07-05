package org.hablapps
package translation

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import cats.data.{Reader => CReader}
import workflows.Translate
import classes._
import lib._

object Main {

  type Env = SQLContext
  type Program[A] = CReader[Env, A]

  // Create workflow

  val parquetReader = new ParquetReader[Env](identity)
  val parquetWriter = new ParquetWriter[Env](identity)
  val workflow = Translate[Program](
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

}
