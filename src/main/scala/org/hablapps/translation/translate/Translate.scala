package org.hablapps
package translation
package translate

import cats.Monad, cats.syntax.all._
import cats.data.{Reader => ReaderC}
import org.hablapps.etl.rdd._
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import readConfig.classes.TranslateColumnConf2
import logic.{TranslateField2DF, SplitTranslation, LogErrors}

import org.hablapps.etl.df.reader.Reader
import org.hablapps.etl.df.writer.Writer

case class Translate[P[_], Input, Lookup](
  ReadInput: Reader[P, Input],
  ReadLookup: Reader[P, Lookup],
  SaveTranslation: Writer[P, Any], // TODO(jfuentes): Input + Translation
  SaveDiscarded: Writer[P, Any]){ // TODO(jfuentes): Input + Translation Errors

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
      _ <- LogErrors(translationResult.discarded, translateColumns)
              .foreach(_.foreach(println))
              .pure
    } yield ()
}
