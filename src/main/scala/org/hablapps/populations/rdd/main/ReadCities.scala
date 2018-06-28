package org.hablapps
package populations
package rdd
package main

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import cats.data.{ Reader => CReader }

import org.hablapps.etl.rdd.reader.instances._

import workflow.City

object ReadCities extends MapReader[City]
