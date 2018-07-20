package org.hablapps

package object etl extends TransformDefs{

  import org.apache.spark.sql.SQLContext
  import org.apache.spark.SparkContext

  implicit def sqlContextFrom(implicit sc: SparkContext) =
    new SQLContext(sc)

  //


  // Utilities for Schemas

  import scala.reflect.runtime.universe.TypeTag
  import org.apache.spark.sql.types.StructType
  import org.apache.spark.sql.catalyst.ScalaReflection

  def product[A <: Product : TypeTag]: StructType =
    ScalaReflection.schemaFor[A].dataType.asInstanceOf[StructType]
}
