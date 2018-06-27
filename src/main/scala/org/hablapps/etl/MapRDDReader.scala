package org.hablapps.etl

import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD

import org.apache.spark.SparkContext

import cats.data.{Reader => CReader, ValidatedNel, Validated}
import cats.Functor

import MapRDDReader.Env

abstract class MapRDDReader[T](implicit CT: ClassTag[T])
extends RDDReader[CReader[Env, ?], T]{
  implicit val F: Functor[CReader[Env, ?]] = implicitly

  type Data = T

  def parse(data: Data): ValidatedNel[Error, T] =
    Validated.valid(data)

  def load(from: String): CReader[Env, RDD[Data]] =
    CReader{
      case (data, sc) => sc.parallelize(data(from).asInstanceOf[Seq[T]])
    }
}

object MapRDDReader{
  type Env = (Map[String,Seq[_]], SparkContext)
}