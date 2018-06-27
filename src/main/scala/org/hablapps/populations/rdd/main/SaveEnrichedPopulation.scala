package org.hablapps.etl
package populations
package main

import workflow.EnrichedPopulation

object SaveEnrichedPopulations extends PhoenixWriter[EnrichedPopulation]{
  val Columns = List("CITY", "P.STATE", "P.POPULATION")
}