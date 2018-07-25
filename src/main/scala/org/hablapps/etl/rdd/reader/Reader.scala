package org.hablapps
package etl
package rdd
package reader

import scala.reflect.{classTag, ClassTag}

import org.apache.spark.rdd.RDD

import cats.data.{ValidatedNel, Validated}, Validated.{Invalid, Valid}
import cats.Functor, cats.syntax.functor._

abstract class Reader[P[_]: Functor, T: ClassTag] extends etl.Reader[RDD,P,T]{

  def parse(data: Data): ValidatedNel[Error, T]

  def apply(from: String): P[RDD[Validated[(Data,List[Error]), T]]] =
    load(from).map(
      _.map( data => parse(data).leftMap(nel => (data,nel.toList))))

  def valid(from: String): P[RDD[T]] =
    apply(from).map(
      _.collect{ case Valid(value) => value })

  def invalid(from: String): P[RDD[(Data, List[Error])]] =
    apply(from).map(
      _.collect{ case Invalid(error) => error })
}