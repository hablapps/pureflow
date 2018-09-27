package org.hablapps
package translation
package translate

import cats.Monad, cats.syntax.all._
import cats.data.{Reader => ReaderC}
import org.hablapps.etl.rdd._
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import readConfig.classes.TranslateColumnConf2
import logic.{TranslateField2DFAPI, SplitTranslationAPI, LogErrorsAPI, Foo}

import org.hablapps.etl.Reader
import org.hablapps.etl.Writer

case class Translate[P[_], Col[_], Input, Lookup](
  ReadInput: Reader[Col, P, Input],
  ReadLookup: Reader[Col, P, Lookup],
  SaveTranslation: Writer[Col, P, Dynamic], // TODO(jfuentes): Input + Translation
  SaveDiscarded: Writer[Col, P, Dynamic], // TODO(jfuentes): Input + Translation Errors
  TranslateField: TranslateField2DFAPI[Col, Dynamic, Dynamic],
  SplitTranslation: SplitTranslationAPI[Col, Dynamic],
  LogErrors: LogErrorsAPI[Col, Dynamic]){

  def run(
      inputSrc: String,
      translateColumns: List[TranslateColumnConf2],
      translationSrc: String,
      discardedSrc: String)(implicit
      M: Monad[P]): P[Unit] =
    for {
      input <- ReadInput.valid(inputSrc)
      translation <-
        translateColumns.foldLeft(input.asInstanceOf[Col[Dynamic]].pure) { (accP, tc) =>
          for {
            lookup <- ReadLookup.valid(tc.lookupSrc)
            acc <- accP
          } yield TranslateField(acc, lookup.asInstanceOf[Col[Dynamic]])(Foo(
            tc.inputColumn,
            tc.lookupKeyColumn,
            tc.lookupValueColumn,
            tc.outputColumn,
            tc.ioId))
        }
      translationResult <- SplitTranslation(translation, translateColumns).pure
      _ <- SaveTranslation.write(translationResult.translated, translationSrc)
      _ <- SaveDiscarded.write(translationResult.discarded, discardedSrc)
      _ <- LogErrors(translationResult.discarded, translateColumns)
              .foreach(_.foreach(println))
              .pure
    } yield ()
}
