package org.hablapps
package translation
package lib

import org.hablapps.etl.Writer
import org.hablapps.etl.df.DataPhrame
import org.apache.spark.sql.DataFrame
import cats.data.State

class ListDFWriter[Env](dfPut: DataFrame => Env => Env) extends Writer[DataPhrame, State[Env, ?], Dynamic] {

  def write(dataframe: DataFrame, destination: String): State[Env, Unit] = State { env =>
    (dfPut(dataframe)(env), ())
  }
}
