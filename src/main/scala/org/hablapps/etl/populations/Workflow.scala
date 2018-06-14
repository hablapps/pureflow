package org.hablapps.etl
package v0

import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext

object Workflow{

  def apply(sc: SparkContext, hc: HBaseContext): Unit = {
    val cities = ReadCities(sc)
    val populations = ReadPopulations.valid("populations.parquet")(new SQLContext(sc))
    val enrichedPopulations = EnrichPopulations(populations, cities)
    SaveEnrichedPopulations(hc, enrichedPopulations)
  }

}