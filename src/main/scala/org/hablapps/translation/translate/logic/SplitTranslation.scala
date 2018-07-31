package org.hablapps
package translation
package translate
package logic

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, not}
import readConfig.classes.TranslateColumnConf2

object SplitTranslation {

  def apply(
      translation: DataFrame,
      translateColumns: List[TranslateColumnConf2]): TranslationResult = {

    val hasErrors = translateColumns.foldLeft(lit(false)) { (acc, tc) =>
      acc || (col(tc.outputColumn) <=> "ERROR")
    }

    translation.persist()

    val result = TranslationResult(translation.where(not(hasErrors)), translation.where(hasErrors))

    translation.unpersist()

    result
  }
}
