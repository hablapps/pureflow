package org.hablapps.etl
package v0

import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.spark.rdd.RDD
import org.apache.phoenix.spark._

object SaveEnrichedPopulations{

  def apply(hc: HBaseContext, rdd: RDD[EnrichedPopulation]): Unit = {
    rdd.saveToPhoenix(tableName = "population",
      cols = List("CITY", "P.STATE", "P.POPULATION"),
      conf = hc.config)
  }
}
