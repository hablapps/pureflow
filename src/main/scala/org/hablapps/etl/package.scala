package org.hablapps

package object etl{
  
  import org.apache.spark.sql.SQLContext
  import org.apache.spark.SparkContext

  implicit def sqlContextFrom(implicit sc: SparkContext) = 
    new SQLContext(sc)

}