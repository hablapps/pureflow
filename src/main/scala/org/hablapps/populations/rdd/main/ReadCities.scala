package org.hablapps.etl
package populations
package main

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import cats.data.{ Reader => CReader }

import workflow.City

object ReadCities extends MapRDDReader[City]