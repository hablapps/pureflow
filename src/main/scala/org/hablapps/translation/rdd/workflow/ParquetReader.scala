package org.hablapps
package translation
package workflow

import org.hablapps.etl.Reader
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import cats.data.{Reader => ReaderC, State}
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

class ParquetReader[Env](sqlcGet: Env => SQLContext) extends DFReader[ReaderC[Env, ?]] {

  def apply(from: String): ReaderC[Env, DataFrame] = load(from) // TODO(jfuentes): WIP
  def invalid(from: String): ReaderC[Env, DataFrame] = load(from) // TODO(jfuentes): WIP
  def load(from: String): ReaderC[Env, DataFrame] = ReaderC { env => sqlcGet(env).read.parquet(from) }
  def valid(from: String): ReaderC[Env, DataFrame] = load(from) // TODO(jfuentes): WIP
}

class ListDFReader[Env, A <: Product](
    sqlcGet: Env => SQLContext,
    asGet: Env => List[A])(implicit TA: TypeTag[A]) extends DFReader[ReaderC[Env, ?]] {

  def apply(from: String): ReaderC[Env, DataFrame] = load(from) // TODO(jfuentes): WIP
  def invalid(from: String): ReaderC[Env, DataFrame] = load(from) // TODO(jfuentes): WIP
  def load(from: String): ReaderC[Env, DataFrame] = ReaderC { env =>
    val sqlc = sqlcGet(env)
    val as = asGet(env)

    sqlc.createDataFrame(as)
  }
  def valid(from: String): ReaderC[Env, DataFrame] = load(from) // TODO(jfuentes): WIP
}

// TODO: Refactor
class ListDFReaderState[Env, A <: Product](
    sqlcGet: Env => SQLContext,
    asGet: Env => List[A])(implicit TA: TypeTag[A]) extends DFReader[State[Env, ?]] {

  def apply(from: String): State[Env, DataFrame] = load(from) // TODO(jfuentes): WIP
  def invalid(from: String): State[Env, DataFrame] = load(from) // TODO(jfuentes): WIP
  def load(from: String): State[Env, DataFrame] = State { env =>
    val sqlc = sqlcGet(env)
    val as = asGet(env)

    (env, sqlc.createDataFrame(as))
  }
  def valid(from: String): State[Env, DataFrame] = load(from) // TODO(jfuentes): WIP
}
