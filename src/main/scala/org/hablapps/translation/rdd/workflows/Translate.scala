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
import logic.{TranslateField2DF, SplitTranslation}

case class Translate[P[_]](
  ReadInput: DFReader[P],
  ReadLookup: DFReader[P],
  SaveTranslation: DFWriter[P],
  SaveDiscarded: DFWriter[P]){

  def run(
      inputSrc: String,
      translateColumns: List[TranslateColumnConf2],
      translationSrc: String,
      discardedSrc: String)(implicit
      M: Monad[P]): P[Unit] =
    for {
      input <- ReadInput.valid(inputSrc)
      translation <-
        translateColumns.foldLeft(input.pure) { (accP, tc) =>
          for {
            lookup <- ReadLookup.valid(tc.lookupSrc)
            acc <- accP
          } yield new TranslateField2DF(
            tc.inputColumn,
            tc.lookupKeyColumn,
            tc.lookupValueColumn,
            tc.outputColumn,
            tc.ioId)(acc, lookup)
        }
      translationResult <- SplitTranslation(translation, translateColumns).pure
      _ <- SaveTranslation.write(translationResult.translated, translationSrc)
      _ <- SaveDiscarded.write(translationResult.discarded, discardedSrc)
    } yield ()
}
