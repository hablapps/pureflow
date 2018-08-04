package org.hablapps.etl
package rdd
package writer
package instances

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

import cats.MonadState, cats.syntax.functor._
import MapWriter.Env

class MapWriter[P[_]: MonadState[?[_], Env], T]
extends Writer[P, T]{
  type K = String

  def write(dataset: RDD[T], destination: K): P[Unit] =
    MonadState[P, Env].modify {
      _ + (destination -> dataset.collect)
    }
}

object MapWriter{
  type Env = Map[String,Seq[_]]
}
