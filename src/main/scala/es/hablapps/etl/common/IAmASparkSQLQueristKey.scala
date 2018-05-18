package es.hablapps.etl.common

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel

trait IAmASparkSQLQueristKey[K, T] {
  protected val readPath:String
  def query(sqlContext:SQLContext, storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK): RDD[(K, T)]
}
