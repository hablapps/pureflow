package org.hablapps
package translation
package workflows

import cats.Monad, cats.syntax.all._
import cats.data.{Reader => ReaderC}
import org.hablapps.etl.rdd._
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import lib.{DFReader, DFWriter}
import classes._
import logic.TranslateFieldDF

case class Translate[P[_]](
  ReadInput: DFReader[P],
  ReadLookup: DFReader[P],
  SaveTranslation: DFWriter[P]){

  def run(
      inputSrc: String,
      translateColumns: List[TranslateColumnConf],
      translationSrc: String)(implicit
      M: Monad[P]): P[Unit] =
    for {
      input <- ReadInput.valid(inputSrc)
      translated <-
        translateColumns.foldLeft(input.pure) { (accP, tc) =>
          for {
            lookup <- ReadLookup.valid(tc.lookupSrc)
            acc <- accP
          } yield new TranslateFieldDF(
            tc.inputColumn,
            tc.lookupKeyColumn,
            tc.lookupValueColumn,
            tc.outputColumn)(acc, lookup)
        }
      _ <- SaveTranslation.write(translated, translationSrc)
    } yield ()
}
