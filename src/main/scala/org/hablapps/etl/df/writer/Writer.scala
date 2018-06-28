package org.hablapps
package etl
package df

import cats.data.Const

import org.apache.spark.sql.DataFrame

package object writer{

  type Writer[P[_],T] = etl.Writer[Const[DataFrame,?],P,T]
}
