package org.hablapps
package populations
package rdd
package main

import org.apache.spark.sql.Row

import cats.data.ValidatedNel
import cats.syntax.cartesian._

import org.hablapps.etl.rdd._, reader._, instances._

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








