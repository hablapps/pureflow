package org.hablapps.etl
package rdd
package reader
package instances

import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD

import org.apache.spark.SparkContext

import cats.{MonadReader, Functor}, cats.syntax.functor._
import cats.data.{ValidatedNel, Validated}

import MapReader._

class MapReader[
  P[_]: MonadReader[?[_], MapReader.Env],
  T: ClassTag
] extends Reader[P, T]{

  type Data = T
  type S = String

  def parse(data: Data): ValidatedNel[Error, T] =
    Validated.valid(data)

  def load(from: S): P[RDD[Data]] =
    MonadReader[P, MapReader.Env].ask map {
      case (data, sc) => sc.parallelize(data(from).asInstanceOf[Seq[T]])
    }
}

object MapReader{
  type Env = (Map[String,Seq[_]], SparkContext)
}
