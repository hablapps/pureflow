package org.hablapps.etl
package df
package reader
package instances

import scala.reflect.ClassTag, scala.reflect.runtime.universe.TypeTag

import org.apache.spark.sql.{DataFrame, SQLContext, Encoder}
import org.apache.spark.sql.types.StructType

import cats.data.{Reader => CReader, ValidatedNel, Validated}
import cats.Functor

class MapReader[T <: Product : TypeTag](implicit CT: ClassTag[T])
extends Reader[MapReader.Program, T]{

  type Data = T

  val Schema = product[T]
  val ErrorSchema = StructType(Nil)

  val validations: Reader.Validations = Map()

  def load(from: String): MapReader.Program[DataPhrame[Data]] =
    CReader{
      case (data, sqlContext) =>
        sqlContext.createDataFrame(data(from).asInstanceOf[Seq[T]])
    }
}

object MapReader{
  type Env = (Map[String,Seq[_]], SQLContext)
  type Program[T] = CReader[Env, T]
}
