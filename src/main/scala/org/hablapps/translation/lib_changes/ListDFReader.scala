package org.hablapps
package translation
package lib

import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types.StructType
import cats.data.{Reader => ReaderC, State}
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

import org.hablapps.etl.df.reader.Reader

class ListDFReader[Env, A <: Product](
    sqlcGet: Env => SQLContext,
    asGet: Env => List[A])(implicit TA: TypeTag[A]) extends Reader[ReaderC[Env, ?], A] {

  def load(from: String): ReaderC[Env, DataFrame] = ReaderC { env =>
    val sqlc = sqlcGet(env)
    val as = asGet(env)

    sqlc.createDataFrame(as)
  }

  val ErrorSchema: StructType = StructType(List.empty)
  val validations: Reader.Validations = Map.empty
}

// TODO: Refactor
class ListDFReaderState[Env, A <: Product](
    sqlcGet: Env => SQLContext,
    asGet: Env => List[A])(implicit TA: TypeTag[A]) extends Reader[State[Env, ?], A] {

  def load(from: String): State[Env, DataFrame] = State { env =>
    val sqlc = sqlcGet(env)
    val as = asGet(env)

    (env, sqlc.createDataFrame(as))
  }

  val ErrorSchema: StructType = StructType(List.empty)
  val validations: Reader.Validations = Map.empty
}
