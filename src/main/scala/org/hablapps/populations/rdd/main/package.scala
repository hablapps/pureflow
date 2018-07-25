package org.hablapps
package populations
package rdd

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.hadoop.hbase.spark.HBaseContext

import cats.data.{Reader => CReader}

package object main{
  type Program[t] =
    CReader[(Map[String,Seq[_]], SparkContext, SQLContext, HBaseContext),t]
}
