package es.hablapps.etl.common.pipeline

import java.util.concurrent.TimeUnit

import es.hablapps.etl.common.IAmARawGeneric
import es.hablapps.etl.common.enricher.EnricherComponent
import es.hablapps.etl.common.sink.SinkComponent
import es.hablapps.etl.common.source.ReaderComponent
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{Accumulator, HashPartitioner, SparkContext}
import org.slf4j.{Logger, LoggerFactory}

import scala.reflect.ClassTag

trait Pipeline[K <: Product, R <: IAmARawGeneric[K], E <: Product] {
  this: ReaderComponent[K, R]
    with SinkComponent[E]
    with EnricherComponent[K, R, E] =>

  def pipeline : Pipeline

  def runPipeline(sc : SparkContext, hc:HBaseContext)(implicit ctr: ClassTag[R],cte: ClassTag[E], ctk: ClassTag[K]): (RDD[(K, R)],RDD[E]) = pipeline(sc, hc)

  class Pipeline(partitionNumber: Int)
  {

    val LOG: Logger = LoggerFactory.getLogger(classOf[Pipeline])

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
      if (LOG.isDebugEnabled) LOG.debug("reduceByKey has reduced {} keys ... ", reducedByKey.value)


      val enriched: RDD[E] = enricher.enrich(reducedRaw)

      if (LOG.isDebugEnabled) LOG.debug("Starting the enrichment")

      sink.sinkPhoenix(hc, enriched)

      if(LOG.isInfoEnabled)
        LOG.info("Time: [{}]secs Counters: [{}] fully enriched [{}] with issues, [{}] critical[{}]",
          TimeUnit.SECONDS.convert(System.nanoTime - startTime, TimeUnit.NANOSECONDS).toString,
          fullyEnriched,
          enrichedWithProblems,
          criticalErrors)

      raw.unpersist(true)
      (raw, enriched)
    }
  }

}

