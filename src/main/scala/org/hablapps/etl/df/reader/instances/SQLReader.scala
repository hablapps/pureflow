package org.hablapps.etl
package df
package reader
package instances

import org.apache.spark.sql.{DataFrame, Row, SQLContext}

import cats.data.{Reader => CReader}

abstract class SQLReader[T: scala.reflect.ClassTag] extends Reader[SQLReader.Program, T]{

  def load(from: String): SQLReader.Program[DataPhrame[Data]] =
    CReader(_.read.load(from))
}

object SQLReader{
  type Program[T] = CReader[SQLContext, T]
}
