package org.hablapps.etl

import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD

import cats.data.{ValidatedNel, Validated}, Validated.{Invalid, Valid}
import cats.Functor, cats.syntax.functor._

trait Reader[P[_],T]{
  implicit val F: Functor[P]
  
  type Data
  type Error

  def parse(data: Data): ValidatedNel[Error, T]

  def load(from: String): P[RDD[Data]]

  def apply(from: String): P[RDD[Validated[(Data,List[Error]), T]]] = 
    load(from).map(
      _.map( data => parse(data).leftMap(nel => (data,nel.toList))))

  def valid(from: String)(implicit
      ct: ClassTag[T]): P[RDD[T]] = 
    apply(from).map(
      _.collect{ case Valid(value) => value })
  
  def invalid(from: String)(implicit 
      ct: ClassTag[T]): P[RDD[(Data, List[Error])]] = 
    apply(from).map(
      _.collect{ case Invalid(error) => error })
}