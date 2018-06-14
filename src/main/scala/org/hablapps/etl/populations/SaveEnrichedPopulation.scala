package org.hablapps.etl
package v0

object SaveEnrichedPopulations extends PhoenixWriter[EnrichedPopulation]{
  val Columns = List("CITY", "P.STATE", "P.POPULATION")
}