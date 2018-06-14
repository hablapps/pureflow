package org.hablapps.etl

import scala.reflect.ClassTag

import org.apache.spark.sql.{Row, DataFrame, SQLContext}
import org.apache.spark.rdd.RDD

import cats.data.{ValidatedNel, Validated}, Validated.{Invalid, Valid}
import cats.syntax.cartesian._

trait Reader[T]{
  type Error

  def parse(row: Row): ValidatedNel[Error, T]

  def apply(source: String)(implicit 
    sqlContext: SQLContext): RDD[Validated[(Row,List[Error]), T]] = 
    sqlContext.read.load(source)
      .map( row => parse(row).leftMap(nel => (row,nel.toList)))

  def valid(source: String)(implicit 
      sqlContext: SQLContext,
      ct: ClassTag[T]): RDD[T] = 
    apply(source)(sqlContext)
      .collect{ case Valid(value) => value }
  
  def invalid(source: String)(implicit 
      sqlContext: SQLContext,
      ct: ClassTag[T]): RDD[(Row, List[Error])] = 
    apply(source)(sqlContext)
      .collect{ case Invalid(error) => error }
}








