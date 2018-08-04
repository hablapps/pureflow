package org.hablapps.etl
package rdd
package writer
package instances

import org.apache.spark.rdd.RDD
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.phoenix.spark._

import cats.MonadReader, cats.syntax.functor._

import scala.reflect.runtime.universe.TypeTag

abstract class PhoenixWriter[
  P[_]: MonadReader[?[_], HBaseContext],
  T <: Product : TypeTag]
extends Writer[P, T]{

  val Columns: List[String]

  type K = String
  def write(rdd: RDD[T], destination: K): P[Unit] =
    MonadReader[P, HBaseContext].ask map { implicit hc =>
      rdd.saveToPhoenix(tableName = destination,
        cols = Columns,
        conf = hc.config)
    }
}