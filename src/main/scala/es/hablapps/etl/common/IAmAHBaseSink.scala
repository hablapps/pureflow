package es.hablapps.etl.common

import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.spark.rdd.RDD

trait IAmAHBaseSink[T] {
  def sinkPhoenix(hBaseContext: HBaseContext, partitionedForHBase : RDD[T]) : Unit
}
