package org.hablapps.etl
package v0

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import cats.data.{ Reader => CReader }

object ReadCities extends MapReader[City]