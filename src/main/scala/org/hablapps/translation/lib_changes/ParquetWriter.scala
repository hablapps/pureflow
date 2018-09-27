package org.hablapps
package translation
package lib

import org.hablapps.etl.Writer
import org.hablapps.etl.df.DataPhrame
import cats.data.{Reader => ReaderC}


class ParquetWriter[Env] extends Writer[DataPhrame, ReaderC[Env, ?], Dynamic] {
  def write(dataset: DataPhrame[Dynamic], destination: String): ReaderC[Env, Unit] = ReaderC { env =>
    dataset.write.parquet(destination)
  }
}
