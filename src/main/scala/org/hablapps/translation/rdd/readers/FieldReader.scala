package org.hablapps
package translation
package readers

import classes.TranslationField
import org.apache.spark.sql.types._
import org.apache.spark.sql.UserDefinedFunction
import org.hablapps.etl.df.reader.Reader
import org.hablapps.etl.df.reader.instances.SQLReader

object FieldReader extends SQLReader[TranslationField] {

  val ErrorSchema: StructType = StructType(List.empty)

  val Schema: StructType = new StructType()
    .add("fieldKey", StringType)
    .add("field", StringType)
    .add("order", StringType)

  val validations: Reader.Validations = Map.empty[StructField, (StructField, UserDefinedFunction)]
}
