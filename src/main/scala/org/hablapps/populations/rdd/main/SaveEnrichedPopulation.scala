package org.hablapps
package populations
package main

import workflow.EnrichedPopulation

import org.hablapps.etl.rdd._

object SaveEnrichedPopulations extends PhoenixWriter[EnrichedPopulation]{
  val Columns = List("CITY", "P.STATE", "P.POPULATION")
}