package org.hablapps.etl
package df
package writer
package instances

import cats.data.Const
import cats.data.State

import org.apache.spark.sql.DataFrame

import MapWriter.Env

abstract class MapWriter[T] extends Writer[State[Env,?],T]{
  def write(dataset: Const[DataFrame,T], destination: String): State[Env,Unit] =
    State{
      map => (map + (destination -> dataset.getConst.collect), ())
    }
}

object MapWriter{
  type Env = Map[String,Seq[_]]
}
