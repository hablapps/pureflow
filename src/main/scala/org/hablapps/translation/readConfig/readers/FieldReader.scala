package org.hablapps
package translation
package readConfig
package readers

import classes.TranslationField
import org.apache.spark.sql.types._
import org.apache.spark.sql.UserDefinedFunction
import org.hablapps.etl.df.reader.Reader
import org.hablapps.etl.df.reader.instances.SQLReader

import cats.MonadReader
import org.apache.spark.sql.SQLContext

class FieldReader[P[_]: MonadReader[?[_], SQLContext]] extends SQLReader[P, TranslationField] {

  val ErrorSchema: StructType = StructType(List.empty)

  // val Schema: StructType = new StructType()
  //   .add("fieldKey", StringType)
  //   .add("field", StringType)
  //   .add("order", StringType)

  val validations: Reader.Validations = Map.empty[StructField, (StructField, UserDefinedFunction)]
}
