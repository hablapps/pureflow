package org.hablapps.etl
package rdd
package writer
package instances

import cats.MonadReader
import cats.syntax.functor._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SaveMode}

import scala.reflect.runtime.universe.TypeTag

abstract class HdfsWriter[
P[_]: MonadReader[?[_], SQLContext],
T <: Product : TypeTag]
  extends Writer[P, T]{

  type K = String
  def write(rdd: RDD[T], destination: K): P[Unit] =
    MonadReader[P, SQLContext].ask map { implicit sQLContext =>
      sQLContext
        .createDataFrame(rdd)
        .write
        .mode(SaveMode.Overwrite)
        .parquet(destination)
    }
  }