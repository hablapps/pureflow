package org.hablapps

package object etl{
  
  import org.apache.spark.sql.SQLContext
  import org.apache.spark.SparkContext

  implicit def sqlContextFrom(implicit sc: SparkContext) = 
    new SQLContext(sc)

  // Reader transformations

  implicit def tuple3to12[A,B,C]: ((A,B,C)) => (A,B) = t => (t._1,t._2)
  implicit def tuple3to3[A,B,C]: ((A,B,C)) => C = t => t._3
}