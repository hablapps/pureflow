package org.hablapps
package translation
package workflow

import org.hablapps.etl.Reader
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import cats.data.{Reader => ReaderC}

trait DFWriter[P[_]] {
  def write(dataframe: DataFrame, destination: String): P[Unit]
}
