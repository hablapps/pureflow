package org.hablapps
package translation

import org.apache.spark.sql.{Row, DataFrame}

package object test {

  implicit class DataFrameTestUtilities(df: DataFrame) { // TODO(jfuentes): This increments compilation time from 2s to 42s?
    def toList[A](f: Row => A): List[A] = df.collect().toList.map(f)
  }

}
