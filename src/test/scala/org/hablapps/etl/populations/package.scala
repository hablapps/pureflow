package org.hablapps.etl
package populations

import cats.data.State
import org.apache.spark.SparkContext

package object test{
  type Program[t] = State[(MapWriter.Env, SparkContext), t]
}

