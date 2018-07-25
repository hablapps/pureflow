package org.hablapps
package translation
package test

import org.scalatest._
import com.holdenkarau.spark.testing.DataFrameSuiteBase

import cats.data.{Reader => CReader, State}

import org.hablapps.etl.rdd._
import org.hablapps.etl._

import org.apache.spark.sql.{DataFrame, SQLContext, Row}

import lib._
import workflows._
import classes._


class WorkflowSpec2 extends FunSpec with Matchers with DataFrameSuiteBase {

  //                      INPUT         LOOKUP          TRANSLATED              DISCARDED
  type Env = (SQLContext, List[Person], List[PersonLU], List[PersonTranslated], List[PersonTranslated])
  type Program[A] = State[Env, A]

  // CREATE WORKFLOW

  val personDFReader = new ListDFReaderState[Env, Person](_._1, _._2)
  val personLUDFReader = new ListDFReaderState[Env, PersonLU](_._1, _._3)
  val translationWriter = new ListDFWriter[Env](df => env => (env._1, env._2, env._3, df.toList(PersonTranslated.fromRow), env._5))
  val discardedWriter = new ListDFWriter[Env](df => env => (env._1, env._2, env._3, env._4, df.toList(PersonTranslated.fromRow)))
  val workflow = Translate[Program](
    personDFReader,
    personLUDFReader,
    translationWriter,
    discardedWriter)

  // COMPILE TO READER

  val program: Program[Unit] = workflow.run(
    "inputSrc",
    List(
      TranslateColumnConf2(
        inputColumn = "name",
        lookupSrc = "foo",
        lookupKeyColumn = "nameKey",
        lookupValueColumn = "nameValue",
        outputColumn = "nameOutput",
        ioId = "5")),
    "outputSrc",
    "discardedSrc")

  describe("Translation") {
    it("should translate correctly") {
      val initialState: Env = (
        sqlContext,
        List(
          Person("Javi", 29, "5"),
          Person("Sulis", 28, "5"),
          Person("Tapi", 2, "555"),
          Person("Pippo", 5, "5")
        ),
        List(
          PersonLU("Javi", "Javier Fuentes", "N/A"),
          PersonLU("Sulis", "Ana Sulistrowski", "N/A"),
          PersonLU("Tapi", "Tapioca", "N/A"),
          PersonLU("Pipo", "Pipoca", "N/A")
        ),
        List.empty,
        List.empty)

      val (_, _, _, translated, discarded) = program.runS(initialState).value

      translated should contain theSameElementsAs List(
        PersonTranslated("Javi", 29, "5", Option("Javier Fuentes")),
        PersonTranslated("Sulis", 28, "5", Option("Ana Sulistrowski")),
        PersonTranslated("Tapi", 2, "555", Option.empty))

      discarded should contain theSameElementsAs List(
        PersonTranslated("Pippo", 5, "5", Option("ERROR")))
    }
  }
}
