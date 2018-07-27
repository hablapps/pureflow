package org.hablapps.etl
package df
package reader
package instances

import scala.reflect.ClassTag

import org.apache.spark.sql.{DataFrame, Row, SQLContext}

import cats.MonadReader, cats.syntax.functor._

abstract class SQLReader[
  P[_]: MonadReader[?[_], SQLContext],
  T: ClassTag]
extends Reader[P, T]{

  def load(from: String): P[DataPhrame[Data]] =
    MonadReader[P, SQLContext].ask map{ _.read.load(from) }
}
