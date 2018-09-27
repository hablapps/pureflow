package org.hablapps
package populations
package rdd
package main

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import cats.MonadReader

import org.hablapps.etl.rdd.reader.instances._

class ReadCities[P[_]: MonadReader[?[_], MapReader.Env]] extends MapReader[P, City]

object ReadCities {
  def apply[P[_]: MonadReader[?[_], MapReader.Env]] = new ReadCities[P]
}
