package org.hablapps
package translation

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._
import cats.data.{Reader => CReader}
import readConfig.ReadConfigWorkflow
import readConfig.classes._
import readConfig.readers._
import translate.Translate
import translate.readers._
import translate.logic.{TranslateField3DF, SplitTranslation, LogErrors}
import lib._

import org.hablapps.etl.df.reader.Reader
import org.hablapps.etl.df.reader.instances.SQLReader
import org.hablapps.etl.Writer
import org.hablapps.etl.df.DataPhrame

object Main extends App {

  type Env = SQLContext
  type Program[A] = CReader[Env, A]

  // ParquetWriter
  val parquetWriter = new ParquetWriter[Env]
  val inputReader = new InputReader[Program]

  // Create read config workflow

  val readConfWF = ReadConfigWorkflow[Program](
    new CriteriaReader[Program],
    new CrossReader[Program],
    new FieldReader[Program])

  // Create translate workflow

  val workflow = Translate[Program, DataPhrame, Dynamic, Dynamic](
    inputReader,
    inputReader,
    parquetWriter,
    parquetWriter,
    TranslateField3DF,
    SplitTranslation,
    LogErrors)

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

  val cfg = new SparkConf().setAppName("translation").setMaster("local")
  val sc = SparkContext.getOrCreate(cfg)
  val sqlContext = new SQLContext(sc)

  compiledProgram(sqlContext)

}
