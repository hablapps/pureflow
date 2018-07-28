package org.hablapps
package translation
package readers

import classes.TranslationCross
import org.apache.spark.sql.types._
import org.apache.spark.sql.UserDefinedFunction
import org.hablapps.etl.df.reader.Reader
import org.hablapps.etl.df.reader.instances.SQLReader

object CrossReader extends SQLReader[TranslationCross] {

  val ErrorSchema: StructType = StructType(List.empty)

  val Schema: StructType = new StructType()
    .add("translationCriteriaId", StringType)
    .add("inputColumn", StringType)
    .add("lookupValueColumn", StringType)
    .add("lookupSrc", StringType)
    .add("origin", StringType)

  val validations: Reader.Validations = Map.empty[StructField, (StructField, UserDefinedFunction)]
}
