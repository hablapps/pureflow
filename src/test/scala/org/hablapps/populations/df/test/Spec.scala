package org.hablapps
package populations
package df
package test

import org.scalatest._
import com.holdenkarau.spark.testing.DataFrameSuiteBase

import cats.data.State, cats.MonadReader

import org.apache.spark.sql.SQLContext

import org.hablapps.etl.df._, reader.instances.MapReader
import org.hablapps.etl._
import naturally.mtl._

class WorkflowSpec extends FunSpec with Matchers with DataFrameSuiteBase{

  // CREATE WORKFLOW

  type Program[t] = State[(Map[String,Seq[_]], SQLContext),t]

  case class ReadPopulations[P[_]: MonadReader[?[_], MapReader.Env]]
  extends reader.instances.MapReader[P, Population]

  val workflow = Workflow[DataPhrame,Program,String,String](
    main.ReadCities[Program],
    ReadPopulations[Program],
    Transforms[Program],
    main.SaveEnrichedPopulations[Program])

  // COMPILE TO READER

  val program: Program[Unit] = workflow.run(
    "cities.seq", "populations.seq", "enrichedpopulations.seq")

  // RUN

  // describe("Person dataframe"){

  //   val personSeq = Seq(
  //     Person("pepe", 40),
  //     Person("isa", -20),
  //     Person(null, 30),
  //     Person("", -35))

  //   def personDF = personSeq.toDF

  //   it("should be validated"){

  //     println("ORIGINAL DATASET")
  //     personDF.show

  //     println("VALIDATED DATASET")
  //     personDF.validate[Person].show

  //     assertDataFrameEquals(
  //       personDF.validate[Person],
  //       Seq(
  //         ("pepe",  40,             null, null: NotDefinedAge,             null,        null),
  //         ( "isa", -20,             null, null: NotDefinedAge, NegativeAge(-20),        null),
  //         (  null,  30, NotDefinedName(), null: NotDefinedAge,             null,        null),
  //         (    "", -35,             null, null: NotDefinedAge, NegativeAge(-35), EmptyName())
  //       ).toDF((Schema.fieldNames ++ Error.Schema.fieldNames): _*))
  //   }
  describe("Populations"){
    it("should be enriched"){
      val (resultingState,_) = program.runS((Map(
        "cities.seq" -> Seq(
          City("Madrid", "MA"),
          City("Barcelona", "BA"),
          City("Zamora", "ZA")),
        "populations.seq" -> Seq(
          Population("Madrid", 3000),
          Population("Barcelona", 2000),
          Population("Zamora", 1000))),
        new SQLContext(sc))).value

      resultingState("enrichedpopulations.seq").toSeq should contain theSameElementsAs Seq(
        EnrichedPopulation("Zamora","ZA",1000),
        EnrichedPopulation("Madrid","MA",3000),
        EnrichedPopulation("Barcelona","BA",2000))
    }
  }
}
