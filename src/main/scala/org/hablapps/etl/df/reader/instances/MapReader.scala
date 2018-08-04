package org.hablapps.etl
package df
package reader
package instances

import scala.reflect.ClassTag, scala.reflect.runtime.universe.TypeTag

import org.apache.spark.sql.{DataFrame, SQLContext, Encoder}
import org.apache.spark.sql.types.StructType

import cats.data.{Reader => CReader, ValidatedNel, Validated}
import cats.{MonadReader, Functor}
import cats.syntax.functor._

abstract class MapReader[
  P[_]: MonadReader[?[_], MapReader.Env],
  T <: Product : TypeTag: ClassTag]
extends Reader[P, T]{

  type Data = T
  type S = String

  val Schema = product[T]
  val ErrorSchema = StructType(Nil)

  val validations: Reader.Validations = Map()

  def load(from: S): P[DataPhrame[Data]] =
    MonadReader[P, MapReader.Env].ask map {
      case (data, sqlContext) =>
        sqlContext.createDataFrame(data(from).asInstanceOf[Seq[T]])
    }
}

object MapReader{
  type Env = (Map[String,Seq[_]], SQLContext)
  // type Program[T] = CReader[Env, T]
}
