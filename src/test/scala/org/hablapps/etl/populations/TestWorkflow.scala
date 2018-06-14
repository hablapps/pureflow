package org.hablapps.etl
package populations
package test

import workflow._

object TestWorkflow extends Workflow[Program]{
  val ReadCities: Reader[Program, City] = main.ReadCities
  val ReadPopulations: Reader[Program, Population] = test.ReadPopulations
  val SaveEnrichedPopulations: Writer[Program, EnrichedPopulation] = test.SaveEnrichedPopulations
}