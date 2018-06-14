package org.hablapps.etl
package populations
package main

import workflow._

object MainWorkflow extends Workflow[Program]{
  val ReadCities: Reader[Program,City] = main.ReadCities
  val ReadPopulations: Reader[Program,Population] = main.ReadPopulations
  val SaveEnrichedPopulations: Writer[Program,EnrichedPopulation] = 
    main.SaveEnrichedPopulations
}
  