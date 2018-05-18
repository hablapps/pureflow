package es.hablapps.etl.population.model

import es.hablapps.etl.common.IAmARawGeneric.Source
import es.hablapps.etl.common.IAmARawGeneric.Source.Source
import org.apache.spark.sql.Row

case class RawPopulation(name: String, population: String) extends IAmARawPopulation {
  override def rawName: String = name
  override def rawPopulation: String = population
  override def srcTable: String = "MEMORY"
  override def source: Source = Source.DATA_EXAMPLE_1
}

object RawPopulation {
  def apply(row: Row): RawPopulation =
    new RawPopulation(
      name = row.getString(0),
      population = row.getString(1)
    )
}

