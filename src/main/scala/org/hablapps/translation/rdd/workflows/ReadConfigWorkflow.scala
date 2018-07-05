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

case class ReadConfigWorkflow[P[_]](
  ReadCriteria: DFReader[P],
  ReadCross: DFReader[P],
  ReadField: DFReader[P]){

  def run(
      criteriaSrc: String,
      crossSrc: String,
      fieldSrc: String)(implicit
      M: Monad[P]): P[DataFrame] =
    for {
      criteria <- ReadCriteria.valid(criteriaSrc)
      cross <- ReadCross.valid(crossSrc)
      field <- ReadField.valid(fieldSrc)
    } yield ParseConf(criteria, cross, field)
}
