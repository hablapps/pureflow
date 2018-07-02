package org.hablapps
package populations
package df
package main

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.udf

import cats.data.ValidatedNel
import cats.syntax.cartesian._

import org.hablapps.etl.df.reader._, instances._

import Population._

object ReadPopulations extends SQLReader[Population]{
  type Data = Row
  type Error = Population.Error

  /** Schemas */

  val Schema = etl.df.product[Population]
  val ErrorSchema = etl.df.product[ErrorRepr]

  case class ErrorRepr(
    negativePopulation: NegativePopulation,
    emptyName: EmptyName)

  /** Validations */

  val validations: Reader.Validations = Map() +
    (ErrorSchema("negativePopulation")->
      (Schema("population"), negativePopulation)) +
    (ErrorSchema("emptyName") ->
      (Schema("name"), emptyName))

  val emptyName = udf((name: String) =>
    if (name != null && name == "") EmptyName() else null)

  val negativePopulation = udf((population: Int) =>
    if (population < 0) NegativePopulation(population) else null)
}








