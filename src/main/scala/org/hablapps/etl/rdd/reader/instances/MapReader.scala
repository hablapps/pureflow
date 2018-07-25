package org.hablapps.etl
package rdd
package reader
package instances

import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD

import org.apache.spark.SparkContext

import cats.data.{Reader => CReader, ValidatedNel, Validated}
import cats.Functor

import MapReader.Env

abstract class MapReader[T](implicit CT: ClassTag[T])
extends Reader[CReader[Env, ?], T]{
  implicit val F: Functor[CReader[Env, ?]] = implicitly

  type Data = T

  def parse(data: Data): ValidatedNel[Error, T] =
    Validated.valid(data)

  def load(from: String): CReader[Env, RDD[Data]] =
    CReader{
      case (data, sc) => sc.parallelize(data(from).asInstanceOf[Seq[T]])
    }
}

object MapReader{
  type Env = (Map[String,Seq[_]], SparkContext)
}
