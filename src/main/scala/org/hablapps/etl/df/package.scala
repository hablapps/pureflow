package org.hablapps
package etl

package object df{

  // DataFrame with Phantom type (kudos to @gneotux)

  import org.apache.spark.sql.DataFrame
  type DataPhrame[_] = DataFrame

  // Utilities

  import scala.reflect.runtime.universe.TypeTag
  import org.apache.spark.sql.types.StructType
  import org.apache.spark.sql.catalyst.ScalaReflection

  def product[A <: Product : TypeTag]: StructType =
    ScalaReflection.schemaFor[A].dataType.asInstanceOf[StructType]
}
