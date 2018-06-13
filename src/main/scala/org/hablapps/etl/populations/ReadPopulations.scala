package org.hablapps.etl
package v0

import org.apache.spark.sql.{Row, DataFrame, SQLContext}
import org.apache.spark.rdd.RDD

object ReadPopulations{

  def PopulationFromRaw(row: Row): Population =
    Population(row.getString(0), row.getString(1))

  def apply(implicit sqlContext: SQLContext): RDD[Population] = 
    sqlContext.createDataFrame(Seq(
      Population("Madrid","2824000"),
      Population("Barcelona","1454000"),
      Population("Valencia","736000"),
      Population("Sevilla","695000"))
    ).map(PopulationFromRaw)
}