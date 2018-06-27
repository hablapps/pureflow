package org.hablapps.etl
package populations

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.hadoop.hbase.spark.HBaseContext

import cats.data.{Reader => CReader}

package object main{
  type Program[t] = 
    CReader[(Map[String,Seq[_]], SparkContext, SQLContext, HBaseContext),t]
}