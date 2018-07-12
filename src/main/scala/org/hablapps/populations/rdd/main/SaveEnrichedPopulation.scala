package org.hablapps
package populations
package rdd
package main

import org.hablapps.etl.rdd.writer.instances._

object SaveEnrichedPopulations extends PhoenixWriter[EnrichedPopulation]{
  val Columns = List("CITY", "P.STATE", "P.POPULATION")
}
