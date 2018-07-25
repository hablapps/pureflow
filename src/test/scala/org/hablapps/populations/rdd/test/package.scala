package org.hablapps
package populations
package rdd

import cats.data.State
import org.apache.spark.SparkContext

import org.hablapps.etl.rdd.writer.instances.MapWriter

package object test{
  type Program[t] = State[(MapWriter.Env, SparkContext), t]
}

