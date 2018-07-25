package org.hablapps.etl
package df
package reader
package instances

import org.apache.spark.sql.{DataFrame, Row, SQLContext}

import cats.data.{Reader => CReader}

trait SQLReader[T] extends Reader[CReader[SQLContext,?],T]{

  def load(from: String): CReader[SQLContext, DataPhrame[Data]] =
    CReader(_.read.load(from))
}
