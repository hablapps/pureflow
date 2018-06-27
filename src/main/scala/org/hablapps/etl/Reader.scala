package org.hablapps.etl

import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD

import cats.data.{ValidatedNel, Validated}, Validated.{Invalid, Valid}
import cats.Functor, cats.syntax.functor._

abstract class Reader[Collection[_], P[_], T] extends java.io.Serializable{
  type Data
  type Error

  def parse(data: Data): ValidatedNel[Error, T]

  def load(from: String): P[Collection[Data]]

  def apply(from: String): P[Collection[Validated[(Data,List[Error]), T]]]

  def valid(from: String): P[Collection[T]]

  def invalid(from: String): P[Collection[(Data, List[Error])]]
}