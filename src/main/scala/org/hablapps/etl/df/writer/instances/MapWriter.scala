package org.hablapps.etl
package df
package writer
package instances

import scala.reflect.runtime.universe.TypeTag

import cats.data.Const
import cats.data.State

import org.apache.spark.sql.SQLContext

import MapWriter.Env

abstract class MapWriter[T <: Product : TypeTag] extends Writer[State[Env,?],T]{
  def write(dataset: DataPhrame[T], destination: String): State[Env,Unit] =
    State{
      case (map,sqlContext) => 
        import sqlContext.implicits._
        ((map + (destination -> dataset.as[T].collect), sqlContext), ())
    }
}

object MapWriter{
  type Env = (Map[String,Seq[_]],SQLContext)
}
