package org.hablapps.etl

import scala.reflect.{classTag, ClassTag}

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql._, expressions._, types._, functions._

import cats.data.{Const, ValidatedNel, Validated}, Validated.{Invalid, Valid}
import cats.Functor, cats.syntax.functor._, cats.syntax.applicative._

abstract class DataFrameReader[P[_]: Functor, T: ClassTag] extends Reader[Const[DataFrame,?],P,T]{

  val Schema: StructType
  val ErrorSchema: StructType

  val validations: DataFrameReader.Validations

  /* Reader API */

  def apply(from: String): P[Const[DataFrame,Validated[(Data,List[Error]), T]]] =
    load(from).map{
      _.modify[Validated[(Data,List[Error]), T]](validate)
    }

  def valid(from: String): P[Const[DataFrame,T]] =
    apply(from) map {
      _.modify[T](filterValid)
    }

  def invalid(from: String): P[Const[DataFrame,(Data, List[Error])]] =
    apply(from) map {
      _.modify[(Data, List[Error])](filterSomeInvalid(_)(ErrorSchema.fieldNames: _*))
    }

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

object DataFrameReader{

  type Validations = Map[StructField, (StructField, UserDefinedFunction)]
}