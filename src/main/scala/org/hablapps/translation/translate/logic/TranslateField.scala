package org.hablapps
package translation
package translate
package logic

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext, DataFrame}
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.types._
import org.hablapps.etl.df.DataPhrame
import scala.reflect.ClassTag


case class TranslationResult(translated: DataFrame, discarded: DataFrame)

class TranslateFieldDF(
    inputColumn: String,
    lookupKeyColumn: String,
    lookupValueColumn: String,
    outputColumn: String) {

  // def apply(input: DataFrame, lookup: DataFrame): DataFrame =
  //   input
  //     .join(
  //       lookup
  //         .select(
  //           col(lookupKeyColumn) as inputColumn,
  //           col(lookupValueColumn) as outputColumn),
  //       List(inputColumn),
  //       "inner")

  // With discarded registers
  def apply2(input: DataFrame, lookup: DataFrame): DataFrame =
    input
      .join(
        lookup
          .select(
            col(lookupKeyColumn) as inputColumn,
            col(lookupValueColumn) as outputColumn),
        List(inputColumn),
        "left_outer")
      .withColumn(
        outputColumn,
        when(col(outputColumn).isNull, lit("ERROR"))
          .otherwise(col(outputColumn)))
}

class TranslateField2DF(
    inputColumn: String,
    lookupKeyColumn: String,
    lookupValueColumn: String,
    outputColumn: String,
    ioId: String) {

  def apply(input: DataFrame, lookup: DataFrame): DataFrame = {
    input.persist()
    val translated = new TranslateFieldDF(inputColumn, lookupKeyColumn, lookupValueColumn, outputColumn).apply2(
      input
        .where(col("IO_ID") === ioId),
      lookup)
    val untranslated = input
      .where(col("IO_ID") !== ioId)
      .withColumn(outputColumn, lit(null))
    input.unpersist()
    translated unionAll untranslated.select(translated.columns.map(col): _*)
  }
}

// TODO(jfuentes): ¿Qué pasa cuando la API es una clase con argumentos de entrada?
// ¿Los muevo al apply? ¿Junto los argumentos en una case class?
// case class Foo(
//   inputColumn: String,
//   lookupKeyColumn: String,
//   lookupValueColumn: String,
//   outputColumn: String,
//   ioId: String)

// abstract class TranslateField2DFAPI[Col[_]](foo: Foo) {
//   def apply(input: Col[Any], lookup: Col[Any]): Col[Any]
// }

abstract class TranslateField2DFAPI[Col[_]](
    inputColumn: String,
    lookupKeyColumn: String,
    lookupValueColumn: String,
    outputColumn: String,
    ioId: String) {
  def apply(input: Col[Any], lookup: Col[Any]): Col[Any]
}

class TranslateField3DF(
    inputColumn: String,
    lookupKeyColumn: String,
    lookupValueColumn: String,
    outputColumn: String,
    ioId: String) extends TranslateField2DFAPI[DataPhrame](inputColumn, lookupKeyColumn, lookupValueColumn, outputColumn, ioId) {

  def apply(input: DataFrame, lookup: DataFrame): DataFrame = {
    input.persist()
    val translated = new TranslateFieldDF(inputColumn, lookupKeyColumn, lookupValueColumn, outputColumn).apply2(
      input
        .where(col("IO_ID") === ioId),
      lookup)
    val untranslated = input
      .where(col("IO_ID") !== ioId)
      .withColumn(outputColumn, lit(null))
    input.unpersist()
    translated unionAll untranslated.select(translated.columns.map(col): _*)
  }
}
