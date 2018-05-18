package es.hablapps.etl.common

import org.apache.spark.rdd.RDD

trait IAmAEnricherComponent[K, R, E] {
  def enrich(raw: RDD[(K, R)]): RDD[E]
}
