package org.hablapps
package populations

import cats.data.State
import org.apache.spark.SparkContext

import org.hablapps.etl.rdd.MapWriter

package object test{
  type Program[t] = State[(MapWriter.Env, SparkContext), t]
}

