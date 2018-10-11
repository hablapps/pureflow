package org.hablapps
package bbva
package rdd.main

import cats.MonadReader
import org.apache.spark.sql.{Row, SQLContext}
import cats.data.ValidatedNel
import cats.syntax.cartesian._
import org.hablapps.etl.rdd._
import reader._
import instances._

case class ReadRetRet[P[_]: MonadReader[?[_], SQLContext]]() extends SQLReader[P, RetRet] with ValidatedHelpers{
  type Error = RetRet.Error

  def parse(row: Row): ValidatedNel[Error, RetRet] =
    (getField(row.getLong(0)) |@|
      getField(row.getString(1)) |@|
      getField(row.getString(2)) |@|
      getField(row.getString(3)) |@|
      getField(row.getString(4)) |@|
      getField(row.getString(5)) |@|
      getField(row.getString(6)) |@|
      getField(row.getShort(7))  |@|
      getField(row.getString(8)) |@|
      getField(row.getString(9)) |@|
      getField(row.getString(10)) |@|
      getField(row.getString(11)) |@|
      getField(row.getString(12))
    ).map(RetRet.apply)

  def getField(field: String): ValidatedNel[Error, String] =
    field.unless(s => s == null || s == "")(RetRet.EmptyField())

  def getField(field: Long): ValidatedNel[Error, Long] =
     field.unless(s => s == null || s < 0)(RetRet.EmptyField())

  def getField(field: Short): ValidatedNel[Error, Short] =
     field.unless(s => s == null || s < 0)(RetRet.EmptyField())
}








