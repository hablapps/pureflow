package org.hablapps

package object etl{
  
  import org.apache.spark.sql.SQLContext
  import org.apache.spark.SparkContext

  implicit def sqlContextFrom(implicit sc: SparkContext) = 
    new SQLContext(sc)

  // Reader transformations

  implicit def tuple4to12[A,B,C,D]: ((A,B,C,D)) => (A,B) = t => (t._1,t._2)
  implicit def tuple4to3[A,B,C,D]: ((A,B,C,D)) => C = t => t._3
  implicit def tuple4to4[A,B,C,D]: ((A,B,C,D)) => D = t => t._4
}