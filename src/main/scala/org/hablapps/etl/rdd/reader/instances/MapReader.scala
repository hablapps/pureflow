package org.hablapps.etl
package rdd
package reader
package instances

import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD

import org.apache.spark.SparkContext

import cats.data.{Reader => CReader, ValidatedNel, Validated}
import cats.Functor

import MapReader._

class MapReader[T: ClassTag]
extends Reader[MapReader.ReaderEnv, T]{

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
  type ReaderEnv[T] = CReader[Env,T]
}
