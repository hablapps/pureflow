package org.hablapps.populations.rdd

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, RDDComparisons, SharedSparkContext}
import org.scalatest.{FlatSpec, Matchers, Suite}

trait SqlContextProvider extends FlatSpec with Matchers with SharedSparkContext with DataFrameSuiteBase with RDDComparisons
