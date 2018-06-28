package org.hablapps.etl
package rdd
package writer
package instances

import org.apache.spark.rdd.RDD
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.phoenix.spark._

import cats.data.{Reader => CReader}

import scala.reflect.runtime.universe.TypeTag

abstract class PhoenixWriter[T <: Product : TypeTag]
extends Writer[CReader[HBaseContext,?], T]{

  val Columns: List[String]

  def write(rdd: RDD[T], destination: String): CReader[HBaseContext,Unit] =
    CReader{ implicit hc =>
      rdd.saveToPhoenix(tableName = destination,
        cols = Columns,
        conf = hc.config)
    }
}
