package org.hablapps
package translation
package logic

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext, DataFrame}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import scala.reflect.ClassTag


object ParseConf {

  def apply(
      criteria: DataFrame,
      cross: DataFrame,
      field: DataFrame): DataFrame = ???
//    input
//      .join(
//        lookup
//          .select(
//            col(lookupKeyColumn) as inputColumn,
//            col(lookupValueColumn) as outputColumn),
//        List(inputColumn),
//        "inner")
}
