package org.hablapps.etl
package rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}

import cats.data.{Reader => CReader}
import cats.Functor

trait SQLReader[T] extends RDDReader[CReader[SQLContext,?],T]{

  type Data = Row

  def load(from: String): CReader[SQLContext, RDD[Data]] =
    CReader( _.read.load(from).rdd )
}