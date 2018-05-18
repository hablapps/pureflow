package es.hablapps.etl

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SQLContext
import org.scalatest.{FlatSpec, Matchers}

trait TestingSuite extends FlatSpec with Matchers with SharedSparkContext {
  lazy val sqlContext = new SQLContext(sc)
  lazy val configuration = new Configuration()
}
