package org.hablapps
package translation
package test

import org.scalatest._
import com.holdenkarau.spark.testing.DataFrameSuiteBase

import cats.data.{Reader => CReader, State}

import org.hablapps.etl.rdd._
import org.hablapps.etl._

import org.apache.spark.sql.{DataFrame, SQLContext, Row}

import workflow._

case class PersonTranslated(name: String, age: Int, nameOutput: String)
object PersonTranslated {
  def fromRow(row: Row): PersonTranslated = row match {
    case Row(name: String, age: Int, nameOutput: String) => PersonTranslated(name, age, nameOutput)
  }
}

class WorkflowSpec2 extends FunSpec with Matchers with DataFrameSuiteBase {

  type Env = (SQLContext, List[Person], List[PersonLU], List[PersonTranslated])
  type Program[A] = State[Env, A]

  // CREATE WORKFLOW

  val personDFReader = new ListDFReaderState[Env, Person](_._1, _._2)
  val personLUDFReader = new ListDFReaderState[Env, PersonLU](_._1, _._3)
  val translationWriter = new ListDFWriter[Env](df => env => (env._1, env._2, env._3, df.toList(PersonTranslated.fromRow)))
  val workflow = WorkflowDF[Program](
    personDFReader,
    personLUDFReader,
    translationWriter)

  // COMPILE TO READER

  val program: Program[Unit] = workflow.run(
    "inputSrc",
    List(
      TranslateColumnConf(
        inputColumn = "name",
        lookupSrc = "foo",
        lookupKeyColumn = "nameKey",
        lookupValueColumn = "nameValue",
        outputColumn = "nameOutput")),
    "outputSrc")

  describe("Translation") {
    it("should translate correctly") {
      val initialState: Env = (
        sqlContext,
        List(
          Person("Javi", 29),
          Person("Sulis", 28),
          Person("Lili", 43)
        ),
        List(
          PersonLU("Javi", "Javier Fuentes", "N/A"),
          PersonLU("Sulis", "Ana Sulistrowski", "N/A"),
          PersonLU("Lolo", "Lili Lolo", "N/A")
        ),
        List.empty)

      val (_, _, _, translated) = program.runS(initialState).value

      translated should contain theSameElementsAs List(
        PersonTranslated("Javi", 29, "Javier Fuentes"),
        PersonTranslated("Sulis", 28, "Ana Sulistrowski"))
    }
  }
}
