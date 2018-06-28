package org.hablapps.etl
package df
package reader
package instances

import scala.reflect.ClassTag, scala.reflect.runtime.universe.TypeTag

import org.apache.spark.sql.{DataFrame, SQLContext, Encoder}
import org.apache.spark.sql.types.StructType

import cats.data.{Const, Reader => CReader, ValidatedNel, Validated}
import cats.Functor

import MapReader.Env

class MapReader[T <: Product : TypeTag](implicit CT: ClassTag[T])
extends Reader[CReader[Env, ?], T]{

  type Data = T
  type Error = Nothing

  val Schema = product[T]
  val ErrorSchema = StructType(Nil)

  val validations: Reader.Validations = Map()

  def load(from: String): CReader[Env, Const[DataFrame,Data]] =
    CReader{
      case (data, sqlContext) =>
        sqlContext.createDataFrame(data(from).asInstanceOf[Seq[T]]).const[Data]
    }
}

object MapReader{
  type Env = (Map[String,Seq[_]], SQLContext)
}
