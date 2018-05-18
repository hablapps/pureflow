package es.hablapps.etl.population.model

import es.hablapps.etl.common.IAmARawGeneric
import es.hablapps.etl.common.IAmARawGeneric.Source.Source

trait IAmARawPopulation extends IAmARawGeneric[PopulationPK] with Serializable{
  def rawName: String
  def rawPopulation: String

  def srcTable: String
  def source: Source

  override def rawPK: PopulationPK = PopulationPK(rawName)

  override def toString: String =
    s"""
       | ${getClass.getSimpleName} (
       | name       = $rawName,
       | population = $rawPopulation
       |)
     """.stripMargin
}
