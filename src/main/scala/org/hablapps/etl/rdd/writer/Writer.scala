package org.hablapps
package etl
package rdd

import org.apache.spark.rdd.RDD

package object writer{

  type Writer[P[_],T] = etl.Writer[RDD,P,T]
}
