package org.hablapps
package etl
package df
package reader

import scala.reflect.{classTag, ClassTag}

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql._, expressions._, types._, functions._

import cats.data.{ValidatedNel, Validated}
import cats.Functor, cats.syntax.functor._

abstract class Reader[P[_]: Functor, T]
extends etl.Reader[DataPhrame, P, T]{

  // val Schema: StructType // TODO(jfuentes): This is not used anywhere. Rethink about it.
  val ErrorSchema: StructType

  val validations: Reader.Validations

  /* Reader API */

  def apply(from: String): P[DataPhrame[Validated[(Data, List[Error]), T]]] =
    load(from) map validate

  def valid(from: String): P[DataPhrame[T]] =
    apply(from) map filterValid

  def invalid(from: String): P[DataPhrame[(Data, List[Error])]] =
    apply(from) map (filterSomeInvalid(_)(ErrorSchema.fieldNames: _*))

  /* DataFrame validation functions */

  def validate(df: DataFrame): DataFrame =
    validations.foldLeft(df){
      case (df, (errorField, (valueField, udf))) =>
        df.withColumn(errorField.name, udf(col(valueField.name)))
    }

  def filterValid(df: DataFrame): DataFrame = {
    val noErrors = ErrorSchema.fieldNames.foldLeft(lit(true)){
      case (result, errorColumnName) =>
        result and df(errorColumnName).isNull
    }

    ErrorSchema.fieldNames.foldLeft(df.filter(noErrors)){
      case (df, errorColumnName) =>
        df.drop(errorColumnName)
    }
  }

  def filterSomeInvalid(df: DataFrame)(invalidColumnNames: String*): DataFrame = {
    val errors = invalidColumnNames.toSeq.foldLeft(lit(false)){
      case (result, errorColumnName) =>
        result or df(errorColumnName).isNotNull
    }

    ErrorSchema.fieldNames.foldLeft(df.filter(errors)){
      case (df, errorColumnName) if ! invalidColumnNames.toSeq.contains(errorColumnName) =>
        df.drop(errorColumnName)
      case (df, _) => df
    }
  }
}

object Reader{

  type Validations = Map[StructField, (StructField, UserDefinedFunction)]
}
