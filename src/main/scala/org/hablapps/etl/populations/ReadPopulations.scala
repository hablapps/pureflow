package org.hablapps.etl
package v0

import org.apache.spark.sql.{Row, DataFrame, SQLContext}
import org.apache.spark.rdd.RDD

import cats.data.{NonEmptyList, ValidatedNel, Validated}, Validated.Valid
import cats.syntax.cartesian._

object ReadPopulations{
  import Population.Error

  def raiseError[T](error: Error): ValidatedNel[Error, T] = Validated.invalidNel(error)
  def returns[T](t: T): ValidatedNel[Error,T] = Validated.valid(t)

  def PopulationFromRaw(row: Row): ValidatedNel[Error, Population] =
    (getName(row.getString(0)) |@|
     getPopulation(row.getLong(1))).map(Population.apply)

  def getName(name: String): ValidatedNel[Error, String] = 
    if (name == "") raiseError(Population.EmptyName())
    else returns(name)

  def getPopulation(population: Long): ValidatedNel[Error, Long] = 
    if (population < 0) raiseError(Population.NegativePopulation(population))
    else returns(population)

  def apply(source: String)(implicit 
    sqlContext: SQLContext): RDD[Validated[(Row,List[Error]), Population]] = 
    sqlContext.read.load(source)
      .map( row => 
        PopulationFromRaw(row)
          .leftMap(nel => (row,nel.toList)))

  def valid(source: String)(implicit sqlContext: SQLContext): RDD[Population] = 
    apply(source)(sqlContext)
      .collect{ case Valid(value) => value }
}








