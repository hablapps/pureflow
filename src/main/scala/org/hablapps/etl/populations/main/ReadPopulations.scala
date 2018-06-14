package org.hablapps.etl
package populations
package main

import org.apache.spark.sql.{Row, DataFrame, SQLContext}
import org.apache.spark.rdd.RDD

import cats.data.{NonEmptyList, ValidatedNel, Validated}, Validated.Valid
import cats.syntax.cartesian._

import workflow.Population

object ReadPopulations extends SQLReader[Population] with ValidatedHelpers{
  type Error = Population.Error

  def parse(row: Row): ValidatedNel[Error, Population] =
    (getName(row.getString(0)) |@|
     getPopulation(row.getLong(1))).map(Population.apply)

  def getName(name: String): ValidatedNel[Error, String] = 
    name.unless(_ == "")(Population.EmptyName())

  def getPopulation(population: Long): ValidatedNel[Error, Long] = 
    population.unless(_ < 0)(Population.NegativePopulation(population))
}








