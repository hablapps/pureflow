package org.hablapps.etl
package rdd
package reader
package instances

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}

import cats.MonadReader, cats.syntax.functor._
import cats.Functor

abstract class SQLReader[
  P[_]: MonadReader[?[_], SQLContext],
  T: scala.reflect.ClassTag
] extends Reader[P, T]{

  type Data = Row
  type S = String

  def load(from: S): P[RDD[Data]] =
    MonadReader[P, SQLContext].ask map {
      _.read.load(from).rdd
    }
}
