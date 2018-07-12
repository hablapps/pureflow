package org.hablapps
package translation
package test

import org.scalatest._
import com.holdenkarau.spark.testing.DataFrameSuiteBase

import cats.data.{Reader => CReader, State}

import org.hablapps.etl.rdd._
import org.hablapps.etl._

import org.apache.spark.sql.{DataFrame, SQLContext}

import lib._
import workflows._
import classes._

case class Person(name: String, age: Int, IO_ID: String)
case class PersonLU(nameKey: String, nameValue: String, timestamp: String)

class WorkflowSpec extends FunSpec with Matchers with DataFrameSuiteBase {

  type Env = (SQLContext, List[Person], List[PersonLU], DataFrame)
  type Program[A] = State[Env, A]
  // type Program[A] = CReader[Env, A]

  // CREATE WORKFLOW

  val personDFReader = new ListDFReaderState[Env, Person](_._1, _._2)
  val personLUDFReader = new ListDFReaderState[Env, PersonLU](_._1, _._3)
  val translationWriter = new ListDFWriter[Env](df => env => (env._1, env._2, env._3, df))
  val workflow = Translate[Program](
    personDFReader,
    personLUDFReader,
    translationWriter)

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
    "outputSrc")

  describe("Translation") {
    it("should be translated") {
      val initialState: Env = (
        sqlContext,
        List(
          Person("Javi", 29, "5"),
          Person("Sulis", 28, "6"),
          Person("Lili", 43, "5")
        ),
        List(
          PersonLU("Javi", "Javier Fuentes", "N/A"),
          PersonLU("Sulis", "Ana Sulistrowski", "N/A"),
          PersonLU("Lolo", "Lili Lolo", "N/A")
        ),
        sqlContext.emptyDataFrame)

      val env2 = program.runS(initialState)
      env2.value._4.show(false)
    }
  }
}
