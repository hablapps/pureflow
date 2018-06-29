package org.hablapps
package translation
package workflow

import org.hablapps.etl.Reader
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import cats.data.{Reader => ReaderC}
import cats.data.State

// class ParquetWriter[P[_]] extends DFWriter[P] {
class ParquetWriter[Env](sqlcGet: Env => SQLContext) extends DFWriter[ReaderC[Env, ?]] {
  def write(dataframe: DataFrame, destination: String): ReaderC[Env, Unit] =
    ReaderC { env =>
      dataframe.write.parquet(destination)
    }
}

class ListDFWriter[Env](dfPut: DataFrame => Env => Env) extends DFWriter[State[Env, ?]] {

  def write(dataframe: DataFrame, destination: String): State[Env, Unit] = State { env =>
    (dfPut(dataframe)(env), ())
  }
}
