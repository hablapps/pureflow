package org.hablapps.etl
package rdd
package writer
package instances

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

import cats.data.State
import MapWriter.Env

abstract class MapWriter[T] extends Writer[MapWriter.Program,T]{
  def write(dataset: RDD[T], destination: String): State[Env,Unit] =
    State{
      map => (map + (destination -> dataset.collect), ())
    }
}

object MapWriter{
  type Env = Map[String,Seq[_]]
  type Program[T] = State[Env, T]
}
