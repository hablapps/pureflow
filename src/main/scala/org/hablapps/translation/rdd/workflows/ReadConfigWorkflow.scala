package org.hablapps
package translation
package workflows

import cats.Monad, cats.syntax.all._
import cats.data.{Reader => ReaderC}
import org.hablapps.etl.rdd._
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import classes._
import logic.ParseConf
import lib._

import org.hablapps.etl.df.reader.Reader

case class ReadConfigWorkflow[P[_]](
  ReadCriteria: Reader[P, TranslationCriteria],
  ReadCross: Reader[P, TranslationCross],
  ReadField: Reader[P, TranslationField]){

  def run(
      processId: String,
      criteriaSrc: String,
      crossSrc: String,
      fieldSrc: String)(implicit
      M: Monad[P]): P[List[TranslateColumnConf2]] =
    for {
      criteria <- ReadCriteria.valid(criteriaSrc)
      cross <- ReadCross.valid(crossSrc)
      field <- ReadField.valid(fieldSrc)
    } yield new ParseConf(processId)(criteria, cross, field)
}
