package org.hablapps.etl
package df
package writer
package instances

import scala.reflect.runtime.universe.TypeTag

import cats.MonadState, cats.syntax.functor._

import org.apache.spark.sql.SQLContext

import MapWriter.Env

class MapWriter[
  P[_]: MonadState[?[_], Env],
  T <: Product : TypeTag]
extends Writer[P, T]{

  def write(dataset: DataPhrame[T], destination: String): P[Unit] =
    MonadState[P, Env].modify {
      case (map,sqlContext) =>
        import sqlContext.implicits._
        (map + (destination -> dataset.as[T].collect), sqlContext)
    }
}

object MapWriter{
  type Env = (Map[String,Seq[_]],SQLContext)
}
