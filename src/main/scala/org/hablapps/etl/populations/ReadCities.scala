package org.hablapps.etl
package v0

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object ReadCities{

  def apply(sc: SparkContext): RDD[City] = 
    sc.parallelize(Seq(
    City("Madrid", "MA"),
    City("Barcelona", "BA"),
    City("Zamora", "ZA")))
}