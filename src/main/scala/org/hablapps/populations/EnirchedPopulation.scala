package org.hablapps
package populations

import java.sql.ResultSet

import org.apache.spark.sql.Row

case class EnrichedPopulation(name: String, shortName: String, population: Long)

object EnrichedPopulation {
  def apply(resultSet: ResultSet):EnrichedPopulation = new EnrichedPopulation(resultSet.getString(1), resultSet.getString(2), resultSet.getLong(3))
  def apply(row: Row):EnrichedPopulation = new EnrichedPopulation(row.getString(0), row.getString(1), row.getLong(2))
}
