package es.hablapps.etl.common.pipeline

import java.util.concurrent.TimeUnit

import es.hablapps.etl.common.IAmARawGeneric
import es.hablapps.etl.common.enricher.EnricherComponent
import es.hablapps.etl.common.sink.SinkComponent
import es.hablapps.etl.common.source.ReaderComponent
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{Accumulator, HashPartitioner, Logging, SparkContext}

import scala.reflect.ClassTag

trait Pipeline[K <: Product, R <: IAmARawGeneric[K], E <: Product] {
  this: ReaderComponent[K, R]
    with SinkComponent[E]
    with EnricherComponent[K, R, E] =>

  def pipeline : Pipeline

  def runPipeline(sc : SparkContext, hc:HBaseContext)(implicit ctr: ClassTag[R],cte: ClassTag[E], ctk: ClassTag[K]): (RDD[(K, R)],RDD[E]) = pipeline(sc, hc)

  class Pipeline(partitionNumber: Int) extends Logging
  {

    def apply(sc: SparkContext, hc: HBaseContext)(implicit ctr: ClassTag[R],cte: ClassTag[E], ctk: ClassTag[K]): (RDD[(K ,R)],RDD[E]) = {

      val startTime: Long = System.nanoTime
      val fullyEnriched: Accumulator[Int] = sc.accumulator(0)
      val enrichedWithProblems: Accumulator[Int] = sc.accumulator(0)
      val criticalErrors: Accumulator[Int] = sc.accumulator(0)
      val failErrors: Accumulator[Int] = sc.accumulator(0)
      val reducedByKey:Accumulator[Int] = sc.accumulator(0)

      val sqlContext = new SQLContext(sc)

      val raw: RDD[(K, R)] = reader.query(sqlContext)

      val reducedRaw = raw.reduceByKey(new HashPartitioner(partitionNumber), (v1, _) => { reducedByKey.add(1) ; v1 })
      logDebug(s"reduceByKey has reduced ${reducedByKey.value} keys ... ")


      val enriched: RDD[E] = enricher.enrich(reducedRaw)

      logDebug("Starting the enrichment")

      sink.sinkPhoenix(hc, enriched)

      logInfo(s"Time: [${TimeUnit.SECONDS.convert(System.nanoTime - startTime, TimeUnit.NANOSECONDS).toString}]secs Counters: " +
        s"[${fullyEnriched}] fully enriched " +
        s"[${enrichedWithProblems}] with issues, " +
        s"[${failErrors}] with errors, " +
        s"[${criticalErrors}] critical")

      raw.unpersist(true)
      (raw, enriched)
    }
  }

}

