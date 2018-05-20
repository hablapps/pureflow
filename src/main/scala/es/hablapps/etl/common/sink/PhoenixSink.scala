package es.hablapps.etl.common.sink

import es.hablapps.etl.common.IAmAHBaseSink
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.spark.rdd.RDD
import org.apache.phoenix.spark._

import scala.reflect.runtime.universe.TypeTag

case class PhoenixSink[T <: Product : TypeTag]() extends IAmAHBaseSink[T] {
  override def sinkPhoenix(hc: HBaseContext, rdd: RDD[T]): Unit = {

    rdd.saveToPhoenix(tableName = "population",
      cols = List("CITY", "P.STATE", "P.POPULATION"),
      conf = hc.config)
  }
}
