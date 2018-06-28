package org.hablapps.etl
package rdd

import cats.data.{ValidatedNel, Validated}, Validated.{Invalid, Valid}
import cats.syntax.cartesian._

trait ValidatedHelpers{
  type Error

  def raiseError[T](error: Error): ValidatedNel[Error, T] =
    Validated.invalidNel(error)

  def returns[T](t: T): ValidatedNel[Error,T] =
    Validated.valid(t)

  implicit class ErrorHelper[T](value: T){
    def unless(cond: T => Boolean)(error: Error): ValidatedNel[Error,T] =
      if (cond(value)) returns(value)
      else raiseError(error)
  }
}
