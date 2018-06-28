package org.hablapps
package etl

package object df{

  import org.apache.spark.sql.DataFrame

  // DataFrame with Phantom type (kudos to @gneotux)
  type DataPhrame[_] = DataFrame
}
