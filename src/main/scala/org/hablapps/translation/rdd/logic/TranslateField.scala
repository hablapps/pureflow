package org.hablapps
package translation
package logic

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext, DataFrame}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types._
import scala.reflect.ClassTag


class TranslateFieldDF(
    inputColumn: String,
    lookupKeyColumn: String,
    lookupValueColumn: String,
    outputColumn: String) {

  def apply(input: DataFrame, lookup: DataFrame): DataFrame =
    input
      .join(
        lookup
          .select(
            col(lookupKeyColumn) as inputColumn,
            col(lookupValueColumn) as outputColumn),
        List(inputColumn),
        "inner")
}

class TranslateField2DF(
    inputColumn: String,
    lookupKeyColumn: String,
    lookupValueColumn: String,
    outputColumn: String,
    ioId: String) {

  def apply(input: DataFrame, lookup: DataFrame): DataFrame = {
    input.persist()
    val translate = new TranslateFieldDF(inputColumn, lookupKeyColumn, lookupValueColumn, outputColumn)(
      input
        .where(col("IO_ID") === ioId),
      lookup)
    val dontTranslate = input
      .where(col("IO_ID") !== ioId)
      .withColumn(outputColumn, lit(null))
    input.unpersist()
    translate unionAll dontTranslate.select(translate.columns.map(col): _*)
  }
}
