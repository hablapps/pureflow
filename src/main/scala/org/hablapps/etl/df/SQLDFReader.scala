package org.hablapps.etl
package df

import org.apache.spark.sql.{DataFrame, Row, SQLContext}

import cats.data.{Const, Reader => CReader}

trait SQLReader[T] extends DataFrameReader[CReader[SQLContext,?],T]{

  def load(from: String): CReader[SQLContext, Const[DataFrame,Data]] =
    CReader(_.read.load(from).const[Data])
}