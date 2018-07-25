package org.hablapps
package translation
package logic

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.col
import classes.TranslateColumnConf2

object LogErrors {

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
    // translateColumns foreach {
    //   case TranslateColumnConf2(inputColumn, lookupSrc, lookupKeyColumn, lookupValueColumn, outputColumn, ioId) =>
    //     discarded
    //       .where(col(outputColumn) === "ERROR")
    //       .select(col(inputColumn))
    //       .mapPartitions { it =>
    //         it map { case Row(inputValue: String) =>
    //           s"There was no translation for input column '$inputColumn' with value '$inputValue'"
    //         }
    //       }
    //       // .select(col(inputColumn))
    //       // .foreach { case Row(inputValue: String) =>
    //       //   println(s"There was no translation for input column '$inputColumn' with value '$inputValue'")
    //       // }
    // }
  }
}
