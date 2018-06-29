package org.hablapps
package translation
package workflow

import org.hablapps.etl.Reader
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import cats.data.{Reader => ReaderC}

trait DFReader[P[_]] extends Reader[Const[DataFrame]#λ, P, Row] {

  type Data = Nothing
  type Error = Nothing

  def apply(from: String): P[DataFrame]
  def invalid(from: String): P[DataFrame]
  def load(from: String): P[DataFrame]
  def valid(from: String): P[DataFrame]
}
