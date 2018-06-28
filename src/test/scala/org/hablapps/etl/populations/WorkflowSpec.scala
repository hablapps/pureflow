package org.hablapps
package populations
package test

import org.scalatest._
import com.holdenkarau.spark.testing.SharedSparkContext

import cats.data.{Reader => CReader, State}

import org.hablapps.etl.rdd._
import org.hablapps.etl._

import workflow._

class WorkflowSpec extends FunSpec with Matchers with SharedSparkContext{

  // CREATE WORKFLOW

  val workflow = Workflow[Program](
    main.ReadCities,
    test.ReadPopulations,
    test.SaveEnrichedPopulations)

  // COMPILE TO READER

  val program: Program[Unit] = workflow.run(
    "cities.seq", "populations.seq", "enrichedpopulations.seq")

  // RUN

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
        sc)).value

      resultingState("enrichedpopulations.seq").toSeq shouldBe Seq(
        EnrichedPopulation("Zamora","ZA",1000),
        EnrichedPopulation("Madrid","MA",3000),
        EnrichedPopulation("Barcelona","BA",2000))
    }
  }
}