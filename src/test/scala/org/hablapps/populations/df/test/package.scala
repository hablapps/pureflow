package org.hablapps
package populations
package df

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

import cats.data.State

package object test{
  type Program[t] =
    State[(Map[String,Seq[_]], SQLContext),t]
}
