package org.hablapps
package translation
package translate
package logic

import etl.df.DataPhrame
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, not}
import readConfig.classes.TranslateColumnConf2

abstract class SplitTranslationAPI[Col[_], Schema] {
  def apply(
    translation: Col[Schema],
    translateColumns: List[TranslateColumnConf2]): TranslationResult[Col, Schema]
}

object SplitTranslation extends SplitTranslationAPI[DataPhrame, Dynamic] {

  def apply(
      translation: DataFrame,
      translateColumns: List[TranslateColumnConf2]): TranslationResult[DataPhrame, Dynamic] = {

    val hasErrors = translateColumns.foldLeft(lit(false)) { (acc, tc) =>
      acc || (col(tc.outputColumn) <=> "ERROR")
    }

    translation.persist()

    val result = TranslationResult[DataPhrame, Dynamic](translation.where(not(hasErrors)), translation.where(hasErrors))

    translation.unpersist()

    result
  }
}
