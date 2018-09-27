package org.hablapps
package translation
package translate
package logic

import etl.df.DataPhrame
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.col
import readConfig.classes.TranslateColumnConf2

abstract class LogErrorsAPI[Col[_], Schema] {
  def apply(
    discarded: Col[Schema],
    translateColumns: List[TranslateColumnConf2]): Option[RDD[String]]
}

object LogErrors extends LogErrorsAPI[DataPhrame, Dynamic] {

  def apply(
      discarded: DataFrame,
      translateColumns: List[TranslateColumnConf2]): Option[RDD[String]] = {

    translateColumns.foldLeft(Option.empty[RDD[String]]) {
      case (accOpt, TranslateColumnConf2(inputColumn, _, _, _, outputColumn, _)) =>
        val foo = discarded
          .where(col(outputColumn) === "ERROR")
          .select(col(inputColumn))
          .mapPartitions { it =>
            it map { case Row(inputValue: String) =>
              s"There was no translation for input column '$inputColumn' with value '$inputValue'"
            }
          }
        accOpt.fold(Option(foo))(acc => Option(acc ++ foo))
    }
  }
}
