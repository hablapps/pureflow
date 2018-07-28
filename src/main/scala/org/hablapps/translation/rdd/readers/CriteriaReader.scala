package org.hablapps
package translation
package readers

import classes.TranslationCriteria
import org.apache.spark.sql.types._
import org.apache.spark.sql.UserDefinedFunction
import org.hablapps.etl.df.reader.Reader
import org.hablapps.etl.df.reader.instances.SQLReader

object CriteriaReader extends SQLReader[TranslationCriteria] {

  val ErrorSchema: StructType = StructType(List.empty)

  val Schema: StructType = new StructType()
    .add("processId", StringType)
    .add("ioId", StringType)
    .add("translationCriteriaId", StringType)
    .add("fieldKey", StringType)

  val validations: Reader.Validations = Map.empty[StructField, (StructField, UserDefinedFunction)]
}