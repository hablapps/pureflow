package org.hablapps
package translation
package readers

import classes.TranslationCriteria
import org.apache.spark.sql.types._
import org.apache.spark.sql.UserDefinedFunction
import org.hablapps.etl.df.reader.Reader
import org.hablapps.etl.df.reader.instances.SQLReader

object InputReader extends SQLReader[Any] {

  val ErrorSchema: StructType = StructType(List.empty)

  val Schema: StructType = StructType(List.empty)

  val validations: Reader.Validations = Map.empty[StructField, (StructField, UserDefinedFunction)]
}
