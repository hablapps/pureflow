package org.hablapps
package translation
package translate
package readers

import readConfig.classes.TranslationCriteria
import org.apache.spark.sql.types._
import org.apache.spark.sql.UserDefinedFunction
import org.hablapps.etl.df.reader.Reader
import org.hablapps.etl.df.reader.instances.SQLReader

import cats.MonadReader
import org.apache.spark.sql.SQLContext

class InputReader[P[_]: MonadReader[?[_], SQLContext]] extends SQLReader[P, Dynamic] {

  val ErrorSchema: StructType = StructType(List.empty)

  // val Schema: StructType = StructType(List.empty)

  val validations: Reader.Validations = Map.empty[StructField, (StructField, UserDefinedFunction)]
}
