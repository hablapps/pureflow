package org.hablapps.etl
package rdd
package reader
package instances

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}

import cats.data.{Reader => CReader}
import cats.Functor

abstract class SQLReader[T: scala.reflect.ClassTag] extends Reader[SQLReader.SQLContextReader, T]{

  type Data = Row

  def load(from: String): CReader[SQLContext, RDD[Data]] =
    CReader( _.read.load(from).rdd )
}

object SQLReader{
  type SQLContextReader[T] = CReader[SQLContext, T]
}
