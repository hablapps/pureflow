package org.hablapps
package translation
package readConfig
package logic

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext, DataFrame}
import org.apache.spark.sql.functions.{col, concat, lit}
import org.apache.spark.sql.types._
import scala.reflect.ClassTag
import classes.TranslateColumnConf2


class ParseConf(processId: String) {

  def apply(
      criteria: DataFrame,
      cross: DataFrame,
      field: DataFrame): List[TranslateColumnConf2] =
    criteria.where(col("process_id") === processId)
      .join(cross, List("translation_criteria_id"), "inner")
      .select(
        col("name_field"),
        col("path_trplus"),
        lit("shortname"),
        col("name_trfield"),
        concat(lit("OUT_"), col("name_field")),
        col("io_id"))
      .collect().toList
      .map(TranslateColumnConf2.fromRow)
}
