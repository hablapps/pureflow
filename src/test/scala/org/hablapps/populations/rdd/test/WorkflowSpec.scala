package org.hablapps
package populations
package rdd
package test

import org.apache.spark.rdd.RDD

import org.scalatest._
import com.holdenkarau.spark.testing.SharedSparkContext

import cats.data.{Reader => CReader, State}

import org.hablapps.etl.rdd._
import org.hablapps.etl._

import naturally.mtl._

class WorkflowSpec extends FunSpec with Matchers with SharedSparkContext{

  // CREATE WORKFLOW

  val workflow = Workflow[RDD,Program](
    main.ReadCities[Program],
    ReadPopulations[Program],
    // EnrichPopulations[Program],
    Transforms[Program],
    SaveEnrichedPopulations[Program])

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

      resultingState("enrichedpopulations.seq").toSeq should contain theSameElementsAs Seq(
        EnrichedPopulation("Zamora","ZA",1000),
        EnrichedPopulation("Madrid","MA",3000),
        EnrichedPopulation("Barcelona","BA",2000))
    }
  }
}
